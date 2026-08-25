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
const linux = std.os.linux;
const Sched = @import("sched.zig").Sched;
const TaskId = @import("sched.zig").TaskId;
const max_tasks = @import("sched.zig").max_tasks;
const Reactor = @import("reactor.zig").Reactor;
const Account = @import("sched.zig").Account;
const posix = std.posix;
const http = @import("http.zig");

/// Every knob is runtime-settable so a sweep needs no recompile. Defaults
/// match the values the earlier runs used.
const Knobs = struct {
    work_budget: i64 = 1000,
    cleanup_reserve: i64 = 50,
    quantum_units: i64 = 250,
    idle_deadline_ms: i64 = 3000,
    bg_budget: i64 = 400,
    bg_period_ms: i64 = 50,
    bg_quantum: i64 = 100,
    tick_ms: u64 = 20,
    accept_burst: usize = 512,
};
var K: Knobs = .{};
const listener_task: TaskId = 0;
const background_task: TaskId = 1;

/// The background task is rate-limited exactly the way an seL4 scheduling
/// context is: a budget that refills on a period. It cannot starve connections
/// no matter how much work it wants to do.


const sockaddr_in = extern struct {
    family: u16 = linux.AF.INET,
    port: u16,
    addr: u32,
    zero: [8]u8 = @splat(0),
};

fn sysErr(rc: usize) bool {
    return @as(isize, @bitCast(rc)) < 0;
}

// ---------------------------------------------------------------- the tick
//
// The only asynchrony in the program. The handler touches four atomics and
// clock_gettime; nothing else here has to be async-signal-safe, because
// `in_kernel` plus a pending counter defers all real work to the next kernel
// entry.

const Shared = struct {
    in_kernel: std.atomic.Value(bool) = .init(true),
    pending: std.atomic.Value(u32) = .init(0),
    arrived_ns: std.atomic.Value(i64) = .init(0),
    took_in_kernel: std.atomic.Value(u64) = .init(0),
    took_in_task: std.atomic.Value(u64) = .init(0),
};
var shared: Shared = .{};

fn onTick(_: posix.SIG) callconv(.c) void {
    if (shared.in_kernel.load(.monotonic)) {
        _ = shared.took_in_kernel.fetchAdd(1, .monotonic);
    } else {
        _ = shared.took_in_task.fetchAdd(1, .monotonic);
    }
    if (shared.pending.fetchAdd(1, .acq_rel) == 0) {
        shared.arrived_ns.store(nowNs(), .monotonic);
    }
}

const timeval = extern struct { sec: isize, usec: isize };
const itimerval = extern struct { it_interval: timeval, it_value: timeval };

fn armTimer(ms: u64) void {
    const v: itimerval = .{
        .it_interval = .{ .sec = @intCast(ms / 1000), .usec = @intCast((ms % 1000) * 1000) },
        .it_value = .{ .sec = @intCast(ms / 1000), .usec = @intCast((ms % 1000) * 1000) },
    };
    _ = linux.syscall3(.setitimer, 0, @intFromPtr(&v), 0);
}

fn nowNs() i64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, @intCast(ts.sec)) * 1_000_000_000 + @as(i64, @intCast(ts.nsec));
}

fn nowMs() i64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, @intCast(ts.sec)) * 1000 + @divTrunc(@as(i64, @intCast(ts.nsec)), 1_000_000);
}

const Phase = enum { reading, working, writing, cleanup };
const Ending = enum { ok, budget_exhausted, deadline_missed, cancelled, bad_request, peer_gone };

const Conn = struct {
    fd: i32,
    phase: Phase = .reading,
    ending: Ending = .ok,
    work_left: i64 = 0,
    spent: i64 = 0,
    parser: http.Parser = .{},
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
    free_hint: TaskId = 2,
    quiet: bool = true,

    steps: u64 = 0,
    accepted: u64 = 0,
    bg_iters: u64 = 0,
    bg_sink: u64 = 0,
    bg_starved: u64 = 0,
    ticks_drained: u64 = 0,
    tick_coalesced: u64 = 0,
    defer_max_ns: i64 = 0,
    served: u64 = 0,

    // ----------------------------------------------------------- the kernel

    fn step(a: *App) void {
        a.steps += 1;

        // 0. Drain the tick. Read the timestamp before clearing the counter so
        //    a signal landing in the window over-reports latency rather than
        //    under-reporting it.
        const arrived = shared.arrived_ns.load(.monotonic);
        const drained = shared.pending.swap(0, .acquire);
        if (drained != 0) {
            const lat = nowNs() - arrived;
            if (lat > a.defer_max_ns) a.defer_max_ns = lat;
            a.ticks_drained += 1;
            a.tick_coalesced += drained;
        }

        // 1. Run everything that can run. O(ready), not O(max_tasks).
        shared.in_kernel.store(false, .release);
        while (a.s.popRunnable()) |t| a.run(t);
        shared.in_kernel.store(true, .release);

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
        if (t == background_task) return a.stepBackground(t);
        const c = &(a.conns[t] orelse return);

        // THE FIX. Previously this checked only `.deadline`, and only that
        // condition. A cancelled task in `.writing` would never notice:
        // stepWriting calls no `charge`, so a zeroed budget is invisible to
        // it, and it would keep writing forever.
        //
        // Both terminating conditions are now checked unconditionally, before
        // the phase switch, in runtime code the app never edits. Per-phase
        // checking is the same shape of mistake as per-call-site `catch`.
        if (c.phase != .cleanup) {
            if (a.s.isCancelled(t)) return a.enterCleanup(t, c, .cancelled);
            if (a.s.reasonFor(t) == .deadline) return a.enterCleanup(t, c, .deadline_missed);
        }
        switch (c.phase) {
            .reading => a.stepReading(t, c),
            .working => a.stepWorking(t, c),
            .writing, .cleanup => a.stepWriting(t, c),
        }
    }

    // ------------------------------------------------------------- listener

    fn doAccept(a: *App) void {
        const t0 = nowNs();
        defer a.s.observe(.accept, nowNs() - t0);
        var burst: usize = 0;
        while (burst < K.accept_burst) : (burst += 1) {
            const rc = linux.accept4(a.listener, null, null, linux.SOCK.NONBLOCK);
            if (sysErr(rc)) break;
            const fd: i32 = @intCast(rc);
            const t = a.freeTask() orelse {
                _ = linux.close(fd);
                break;
            };
            a.conns[t] = .{ .fd = fd };
            a.s.admit(t, K.work_budget, K.cleanup_reserve);
            a.s.arm(t, nowMs() + K.idle_deadline_ms);
            a.accepted += 1;
        }
        a.r.watch(listener_task, a.listener, .read);
    }

    /// A CPU-bound task with no I/O at all. It exists to give the tick
    /// something to preempt, and to show that background work throttled by a
    /// refilling budget cannot starve the connections it shares a core with.
    fn stepBackground(a: *App, t: TaskId) void {
        // A refill deadline arrived: top the budget back up. Same wheel, same
        // mechanism as a connection's idle timeout -- only the meaning differs.
        if (a.s.reasonFor(t) == .deadline) {
            a.s.refund(t, K.bg_budget, 0);
            a.s.arm(t, nowMs() + K.bg_period_ms);
        }

        const t0 = nowNs();
        if (!a.s.chargeTo(t, .background, K.bg_quantum)) {
            // Out of budget for this period. Park until the refill fires --
            // it does not spin, and it does not steal from connections.
            a.bg_starved += 1;
            a.s.observe(.background, nowNs() - t0);
            return;
        }
        // Sequentially dependent so it cannot be folded to a closed form.
        var j: i64 = 0;
        while (j < K.bg_quantum * 2000) : (j += 1)
            a.bg_sink = a.bg_sink *% 6364136223846793005 +% 1442695040888963407;
        a.bg_iters += 1;
        a.s.observe(.background, nowNs() - t0);
        a.s.makeRunnable(t, .spawn);
    }

    fn freeTask(a: *App) ?TaskId {
        var i = a.free_hint;
        var n: usize = 0;
        while (n < max_tasks - 1) : (n += 1) {
            if (a.conns[i] == null) {
                a.free_hint = if (i + 1 >= max_tasks) 2 else i + 1;
                return i;
            }
            i = if (i + 1 >= max_tasks) 2 else i + 1;
        }
        return null;
    }

    // --------------------------------------------------------------- phases

    fn park(a: *App, t: TaskId, c: *Conn, i: @import("reactor.zig").Interest) void {
        a.r.watch(t, c.fd, i);
    }

    /// The driver: I/O and budget, nothing else. All protocol knowledge lives
    /// in http.Parser, which never sees this fd.
    ///
    /// The unconsumed tail must survive across requests. `feed` returning less
    /// than it was given is the normal pipelined case, and dropping the
    /// remainder silently loses the next request -- which is precisely what
    /// the first version of this function did.
    fn stepReading(a: *App, t: TaskId, c: *Conn) void {
        // Buffered bytes from a previous read may already hold a full request.
        if (c.in_len > 0 and a.drainParser(t, c)) return;

        if (c.in_len == c.in.len) return a.enterCleanup(t, c, .bad_request);
        const rc = linux.read(c.fd, c.in[c.in_len..].ptr, c.in.len - c.in_len);
        if (sysErr(rc)) return a.park(t, c, .read);
        if (rc == 0) return a.finish(t, c, .peer_gone);
        c.in_len += rc;

        if (a.drainParser(t, c)) return;
        a.park(t, c, .read);
    }

    /// Feeds buffered bytes to the parser, consuming exactly what it takes and
    /// keeping the rest. Returns true if the connection changed phase.
    fn drainParser(a: *App, t: TaskId, c: *Conn) bool {
        const t0 = nowNs();
        defer a.s.observe(.parse, nowNs() - t0);
        while (c.in_len > 0) {
            const used = c.parser.feed(c.in[0..c.in_len]);
            if (used > 0) {
                std.mem.copyForwards(u8, c.in[0 .. c.in_len - used], c.in[used..c.in_len]);
                c.in_len -= used;
                // Charged against bytes the machine says it consumed -- an
                // honest quantity, not a made-up constant.
                const units = @divTrunc(@as(i64, @intCast(used)), 64) + 1;
                if (!a.s.chargeTo(t, .parse, units)) {
                    a.enterCleanup(t, c, .budget_exhausted);
                    return true;
                }
            }
            switch (c.parser.poll()) {
                .need_input => if (used == 0) return false else continue,
                .protocol_error => {
                    a.enterCleanup(t, c, .bad_request);
                    return true;
                },
                .request => |req| {
                    c.work_left = req.work_units;
                    c.phase = .working;
                    a.s.makeRunnable(t, .spawn);
                    return true;
                },
            }
        }
        return false;
    }

    fn stepWorking(a: *App, t: TaskId, c: *Conn) void {
        const t0 = nowNs();
        defer a.s.observe(.work, nowNs() - t0);
        const charge = @min(K.quantum_units, c.work_left);
        if (charge > 0 and !a.s.chargeTo(t, .work, charge)) {
            return a.enterCleanup(t, c, .budget_exhausted);
        }
        c.spent += charge;
        c.work_left -= charge;
        var j: i64 = 0;
        while (j < charge * 200) : (j += 1)
            c.sink = c.sink *% 6364136223846793005 +% 1442695040888963407;

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
        const t0 = nowNs();
        defer a.s.observe(.cleanup, nowNs() - t0);
        c.phase = .cleanup;
        a.s.chargeReserve(t, 10);
        // A cancelled task parked in the interest set must come out of it.
        // That unwatch is itself a cleanup-reserve action.
        a.r.unwatch(t);
        a.s.arm(t, nowMs() + 500);
        const body = switch (why) {
            .bad_request => "400 bad request      \n",
            .budget_exhausted => "503 budget exhausted\n",
            .cancelled => "503 cancelled        \n",
            else => "408 deadline missed\n",
        };
        const status = switch (why) {
            .bad_request => "400 Bad Request",
            .budget_exhausted => "503 Service Unavailable",
            .cancelled => "503 Service Unavailable",
            else => "408 Request Timeout",
        };
        c.out_sent = 0;
        c.out_len = (std.fmt.bufPrint(&c.out, "HTTP/1.1 {s}\r\nContent-Length: {d}\r\n\r\n{s}", .{ status, body.len, body }) catch
            return a.finish(t, c, why)).len;
        a.s.makeRunnable(t, .spawn);
    }

    fn stepWriting(a: *App, t: TaskId, c: *Conn) void {
        const t0 = nowNs();
        defer a.s.observe(.write, nowNs() - t0);
        const rc = linux.write(c.fd, c.out[c.out_sent..].ptr, c.out_len - c.out_sent);
        if (sysErr(rc)) return a.park(t, c, .write);
        c.out_sent += rc;
        if (c.out_sent < c.out_len) return a.park(t, c, .write);

        if (c.phase == .cleanup) return a.finish(t, c, c.ending);

        // keep-alive: same task, fresh budget and deadline
        a.served += 1;
        // Preserve any pipelined bytes already buffered; resetting them here
        // is the same bug in a different place.
        var keep_in: [512]u8 = undefined;
        const keep_len = c.in_len;
        @memcpy(keep_in[0..keep_len], c.in[0..keep_len]);
        c.* = .{ .fd = c.fd };
        @memcpy(c.in[0..keep_len], keep_in[0..keep_len]);
        c.in_len = keep_len;

        a.s.refund(t, K.work_budget, K.cleanup_reserve);
        a.s.arm(t, nowMs() + K.idle_deadline_ms);
        if (c.in_len > 0) return a.s.makeRunnable(t, .spawn); // next request already here
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
    var argv: [24][]const u8 = undefined;
    var argc: usize = 0;
    for (init.args.vector) |x| {
        if (argc == 24) break;
        argv[argc] = std.mem.span(x);
        argc += 1;
    }
    const arg_port: u16 = if (argc > 1) (std.fmt.parseInt(u16, argv[1], 10) catch 0) else 0;
    const arg_secs: i64 = if (argc > 2) (std.fmt.parseInt(i64, argv[2], 10) catch 12) else 12;
    var csv = false;
    for (argv[0..argc]) |arg| {
        if (std.mem.eql(u8, arg, "csv")) { csv = true; continue; }
        const eq = std.mem.indexOfScalar(u8, arg, '=') orelse continue;
        const key = arg[0..eq];
        const val = std.fmt.parseInt(i64, arg[eq + 1 ..], 10) catch continue;
        inline for (@typeInfo(Knobs).@"struct".fields) |f| {
            if (std.mem.eql(u8, key, f.name)) {
                @field(K, f.name) = switch (f.type) {
                    i64 => val,
                    u64 => @intCast(val),
                    usize => @intCast(val),
                    else => @field(K, f.name),
                };
            }
        }
    }

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
    try a.r.init();
    a.r.watch(listener_task, sock, .read);

    // Background task: always runnable, budget refilled on a period.
    a.s.admit(background_task, K.bg_budget, 0);
    a.s.arm(background_task, nowMs() + K.bg_period_ms);

    var act: posix.Sigaction = .{
        .handler = .{ .handler = onTick },
        .mask = posix.sigemptyset(),
        .flags = posix.SA.RESTART,
    };
    posix.sigaction(.ALRM, &act, null);
    armTimer(K.tick_ms);

    std.debug.print("listening on 127.0.0.1:{d}  budget={d} reserve={d} quantum={d}\n", .{
        std.mem.bigToNative(u16, addr.port), K.work_budget, K.cleanup_reserve, K.quantum_units,
    });

    const t_start = nowNs();
    const stop = nowMs() + arg_secs * 1000;
    while (nowMs() < stop) a.step();

    const wall_ns = nowNs() - t_start;
    var observed: i64 = 0;
    for (a.s.ns_by) |v| observed += v;
    const unacc = wall_ns - observed;

    if (csv) {
        // One machine-readable line per run. Everything a sweep needs.
        std.debug.print("SRV,{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d}\n", .{
            K.quantum_units, K.work_budget, K.bg_budget, K.bg_period_ms, K.tick_ms,
            a.steps, a.served, a.r.waits, a.bg_iters, a.bg_starved,
            a.ticks_drained, a.tick_coalesced, a.defer_max_ns,
            a.s.ns_by[@intFromEnum(Account.parse)],
            a.s.ns_by[@intFromEnum(Account.work)],
            a.s.ns_by[@intFromEnum(Account.write)],
            unacc,
        });
        return;
    }

    std.debug.print(
        "\nsteps={d} accepted={d} served={d} epoll_waits={d} avg_armed={d} rearms={d}\n",
        .{ a.steps, a.accepted, a.served, a.r.waits, if (a.r.waits > 0) a.r.fds_polled / a.r.waits else 0, a.s.rearms },
    );
    std.debug.print("background: iters={d} starved={d} (budget {d}/{d}ms)\n", .{ a.bg_iters, a.bg_starved, K.bg_budget, K.bg_period_ms });
    std.debug.print("tick: drained={d} coalesced={d} in_kernel={d} in_task={d} max_defer={d:.1}us\n", .{
        a.ticks_drained, a.tick_coalesced,
        shared.took_in_kernel.load(.monotonic), shared.took_in_task.load(.monotonic),
        @as(f64, @floatFromInt(a.defer_max_ns)) / 1000.0,
    });

    std.debug.print("\n{s:<12} {s:>10} {s:>12} {s:>12} {s:>10} {s:>8}\n", .{ "account", "calls", "units", "ns", "ns/unit", "denied" });
    for (std.enums.values(Account)) |acc| {
        const i = @intFromEnum(acc);
        if (a.s.calls_by[i] == 0 and a.s.ns_by[i] == 0) continue;
        const npu: f64 = if (a.s.units_by[i] > 0)
            @as(f64, @floatFromInt(a.s.ns_by[i])) / @as(f64, @floatFromInt(a.s.units_by[i]))
        else
            0;
        std.debug.print("{s:<12} {d:>10} {d:>12} {d:>12} {d:>10.1} {d:>8}\n", .{
            @tagName(acc), a.s.calls_by[i], a.s.units_by[i], a.s.ns_by[i], npu, a.s.denied_by[i],
        });
    }
    std.debug.print("{s:<12} {s:>10} {s:>12} {d:>12}\n", .{ "wall", "", "", wall_ns });
    std.debug.print("{s:<12} {s:>10} {s:>12} {d:>12}   <- charges no units: syscalls, epoll_wait, scheduler ({d:.1}%)\n", .{
        "UNACCOUNTED", "", "", unacc,
        100.0 * @as(f64, @floatFromInt(unacc)) / @as(f64, @floatFromInt(wall_ns)),
    });
}
