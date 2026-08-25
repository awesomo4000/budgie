//! The application, and the kernel step that ties scheduler to reactor.
//!
//! The step is now four lines and mentions neither fds nor phases:
//!
//!     while (sched.popRunnable()) |t| app.run(t);
//!     const timeout = sched.timeoutMs(now);
//!     reactor.wait(&sched, timeout);
//!     sched.expire(now);
//!
//! Everything socket-shaped lives below `run`. Everything runnability-shaped
//! lives in sched.zig. Swapping poll for epoll touches reactor.zig only.

const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;
const Sched = @import("sched.zig").Sched;
const TaskId = @import("sched.zig").TaskId;
const max_tasks = @import("sched.zig").max_tasks;
const Reactor = @import("reactor.zig").Reactor;

const work_budget: i64 = 1000;
const cleanup_reserve: i64 = 50;
const quantum_units: i64 = 250;
const idle_deadline_ms: i64 = 3000;
const listener_task: TaskId = 0;

const sockaddr_in = extern struct {
    family: u16 = linux.AF.INET,
    port: u16,
    addr: u32,
    zero: [8]u8 = @splat(0),
};

fn sysErr(rc: usize) bool {
    return @as(isize, @bitCast(rc)) < 0;
}

fn nowMs() i64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, @intCast(ts.sec)) * 1000 + @divTrunc(@as(i64, @intCast(ts.nsec)), 1_000_000);
}

const Phase = enum { reading, working, writing, cleanup };
const Ending = enum { ok, budget_exhausted, deadline_missed, peer_gone };

const Conn = struct {
    fd: i32,
    phase: Phase = .reading,
    ending: Ending = .ok,
    work_left: i64 = 0,
    spent: i64 = 0,
    in_len: usize = 0,
    out_len: usize = 0,
    out_sent: usize = 0,
    in: [512]u8 = undefined,
    out: [256]u8 = undefined,
    sink: u64 = 0,
};

const App = struct {
    s: Sched = .{},
    r: Reactor = .{},
    conns: [max_tasks]?Conn = @splat(null),
    listener: i32,
    free_hint: TaskId = 1,
    quiet: bool = true,

    steps: u64 = 0,
    accepted: u64 = 0,
    served: u64 = 0,

    // ----------------------------------------------------------- the kernel

    fn step(a: *App) void {
        a.steps += 1;

        // 1. Run everything that can run. O(ready), not O(max_tasks).
        while (a.s.popRunnable()) |t| a.run(t);

        // 2. How long may we sleep? O(1) peek at the deadline heap.
        const now = nowMs();
        const timeout: i32 = if (a.s.anyRunnable())
            0
        else if (a.s.timeoutMs(now)) |ms|
            @intCast(@min(1000, ms))
        else
            1000;

        // 3. Block. The reactor marks ready tasks runnable.
        _ = a.r.wait(&a.s, timeout);

        // 4. Expire deadlines. O(expired * log n).
        a.s.expire(nowMs());
    }

    fn run(a: *App, t: TaskId) void {
        if (t == listener_task) return a.doAccept();
        const c = &(a.conns[t] orelse return);

        if (a.s.reasonFor(t) == .deadline and c.phase != .cleanup) {
            return a.enterCleanup(t, c, .deadline_missed);
        }
        switch (c.phase) {
            .reading => a.stepReading(t, c),
            .working => a.stepWorking(t, c),
            .writing, .cleanup => a.stepWriting(t, c),
        }
    }

    // ------------------------------------------------------------- listener

    fn doAccept(a: *App) void {
        var burst: usize = 0;
        while (burst < 512) : (burst += 1) {
            const rc = linux.accept4(a.listener, null, null, linux.SOCK.NONBLOCK);
            if (sysErr(rc)) break;
            const fd: i32 = @intCast(rc);
            const t = a.freeTask() orelse {
                _ = linux.close(fd);
                break;
            };
            a.conns[t] = .{ .fd = fd };
            a.s.admit(t, work_budget, cleanup_reserve);
            a.s.arm(t, nowMs() + idle_deadline_ms);
            a.accepted += 1;
        }
        a.r.watch(listener_task, a.listener, .read);
    }

    fn freeTask(a: *App) ?TaskId {
        var i = a.free_hint;
        var n: usize = 0;
        while (n < max_tasks - 1) : (n += 1) {
            if (a.conns[i] == null) {
                a.free_hint = if (i + 1 >= max_tasks) 1 else i + 1;
                return i;
            }
            i = if (i + 1 >= max_tasks) 1 else i + 1;
        }
        return null;
    }

    // --------------------------------------------------------------- phases

    fn park(a: *App, t: TaskId, c: *Conn, i: @import("reactor.zig").Interest) void {
        a.r.watch(t, c.fd, i);
    }

    fn stepReading(a: *App, t: TaskId, c: *Conn) void {
        const rc = linux.read(c.fd, c.in[c.in_len..].ptr, c.in.len - c.in_len);
        if (sysErr(rc)) return a.park(t, c, .read);
        if (rc == 0) return a.finish(t, c, .peer_gone);
        c.in_len += rc;

        const head = std.mem.indexOf(u8, c.in[0..c.in_len], "\r\n") orelse
            return a.park(t, c, .read);
        const line = c.in[0..head];
        c.work_left = blk: {
            const p = std.mem.indexOf(u8, line, "/work/") orelse break :blk 0;
            var end = p + 6;
            while (end < line.len and line[end] >= '0' and line[end] <= '9') end += 1;
            break :blk std.fmt.parseInt(i64, line[p + 6 .. end], 10) catch 0;
        };
        c.phase = .working;
        a.s.makeRunnable(t, .spawn);
    }

    fn stepWorking(a: *App, t: TaskId, c: *Conn) void {
        const charge = @min(quantum_units, c.work_left);
        if (charge > 0 and !a.s.charge(t, charge)) {
            return a.enterCleanup(t, c, .budget_exhausted);
        }
        c.spent += charge;
        c.work_left -= charge;
        var j: i64 = 0;
        while (j < charge * 200) : (j += 1) c.sink +%= @intCast(j);

        if (c.work_left > 0) return a.s.makeRunnable(t, .spawn);

        c.out_len = (std.fmt.bufPrint(&c.out,
            "HTTP/1.1 200 OK\r\nContent-Length: 24\r\n\r\ndone, spent {d:>5} units\n", .{c.spent}) catch
            return a.finish(t, c, .peer_gone)).len;
        c.out_sent = 0;
        c.phase = .writing;
        a.s.makeRunnable(t, .spawn);
    }

    fn enterCleanup(a: *App, t: TaskId, c: *Conn, why: Ending) void {
        c.ending = why;
        c.phase = .cleanup;
        a.s.chargeReserve(t, 10);
        a.s.arm(t, nowMs() + 500);
        const body = if (why == .budget_exhausted) "503 budget exhausted\n" else "408 deadline missed\n";
        const status = if (why == .budget_exhausted) "503 Service Unavailable" else "408 Request Timeout";
        c.out_sent = 0;
        c.out_len = (std.fmt.bufPrint(&c.out, "HTTP/1.1 {s}\r\nContent-Length: {d}\r\n\r\n{s}", .{ status, body.len, body }) catch
            return a.finish(t, c, why)).len;
        a.s.makeRunnable(t, .spawn);
    }

    fn stepWriting(a: *App, t: TaskId, c: *Conn) void {
        const rc = linux.write(c.fd, c.out[c.out_sent..].ptr, c.out_len - c.out_sent);
        if (sysErr(rc)) return a.park(t, c, .write);
        c.out_sent += rc;
        if (c.out_sent < c.out_len) return a.park(t, c, .write);

        if (c.phase == .cleanup) return a.finish(t, c, c.ending);

        // keep-alive: same task, fresh budget and deadline
        a.served += 1;
        c.* = .{ .fd = c.fd };
        a.s.refund(t, work_budget, cleanup_reserve);
        a.s.arm(t, nowMs() + idle_deadline_ms);
        a.park(t, c, .read);
    }

    fn finish(a: *App, t: TaskId, c: *Conn, why: Ending) void {
        if (!a.quiet) std.debug.print("  task {d}: closed [{s}] spent={d} reserve_left={d}\n", .{ t, @tagName(why), c.spent, a.s.reserve[t] });
        a.r.unwatch(t);
        a.s.disarm(t);
        a.s.release(t);
        _ = linux.close(c.fd);
        a.conns[t] = null;
        a.served += 1;
    }
};

var app_storage: App = undefined;

pub fn main(init: std.process.Init.Minimal) !void {
    var argv: [8][]const u8 = undefined;
    var argc: usize = 0;
    for (init.args.vector) |x| {
        if (argc == 8) break;
        argv[argc] = std.mem.span(x);
        argc += 1;
    }
    const arg_port: u16 = if (argc > 1) (std.fmt.parseInt(u16, argv[1], 10) catch 0) else 0;
    const arg_secs: i64 = if (argc > 2) (std.fmt.parseInt(i64, argv[2], 10) catch 12) else 12;

    const src = linux.socket(linux.AF.INET, linux.SOCK.STREAM | linux.SOCK.NONBLOCK, 0);
    if (sysErr(src)) return error.SocketFailed;
    const sock: i32 = @intCast(src);
    const one: c_int = 1;
    _ = linux.setsockopt(sock, linux.SOL.SOCKET, linux.SO.REUSEADDR, @ptrCast(&one), @sizeOf(c_int));
    var addr = sockaddr_in{ .port = std.mem.nativeToBig(u16, arg_port), .addr = 0x0100007f };
    if (sysErr(linux.bind(sock, @ptrCast(&addr), @sizeOf(sockaddr_in)))) return error.BindFailed;
    if (sysErr(linux.listen(sock, 4096))) return error.ListenFailed;
    var len: linux.socklen_t = @sizeOf(sockaddr_in);
    _ = linux.getsockname(sock, @ptrCast(&addr), &len);

    app_storage = .{ .listener = sock };
    const a = &app_storage;
    a.s.live[listener_task] = true;
    a.r.watch(listener_task, sock, .read);

    std.debug.print("listening on 127.0.0.1:{d}  budget={d} reserve={d} quantum={d}\n", .{
        std.mem.bigToNative(u16, addr.port), work_budget, cleanup_reserve, quantum_units,
    });

    const stop = nowMs() + arg_secs * 1000;
    while (nowMs() < stop) a.step();

    std.debug.print(
        "\nsteps={d} accepted={d} served={d} polls={d} avg_pollfds={d} timer_fires={d} rearms={d}\n",
        .{ a.steps, a.accepted, a.served, a.r.waits, if (a.r.waits > 0) a.r.fds_polled / a.r.waits else 0, a.s.fires, a.s.rearms },
    );
}
