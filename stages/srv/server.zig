//! A cooperative kernel that is also a server.
//!
//! The point of this one: when every task is parked on I/O, the kernel is
//! *asleep* in `poll` and burns nothing. No signal is involved. Deadlines are
//! an argument to the wait, not a watchdog.
//!
//! Three things are demonstrated end to end:
//!
//!   1. `step()` is still the whole kernel. It blocks when nothing is
//!      runnable, polls with timeout 0 when something is.
//!
//!   2. Caller-pays budget: each connection is admitted with a fixed work
//!      budget. The request says how much work it wants. Exceeding the budget
//!      is not a crash, it is a transition.
//!
//!   3. A cleanup reserve that the body can never touch. Whether a connection
//!      ends normally, blows its budget, or misses its deadline, the unwind
//!      runs on `reserve`, so cleanup never competes with the work that just
//!      overran.
//!
//! `poll` rather than `select` only because select's fd_set has a 1024 fd
//! ceiling and a fiddlier API; the structure is identical.

const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;

const max_conns = 4096;
const work_budget: i64 = 1000; // units a request body may consume
const cleanup_reserve: i64 = 50; // units reserved for unwind, always granted
const quantum_units: i64 = 250; // work charged per scheduler quantum
const idle_deadline_ms: i64 = 3000;

// std 0.16 moved sockets behind std.Io; raw syscalls keep this self-contained.
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

const Phase = enum { reading, working, writing, cleanup, done };
const Ending = enum { ok, budget_exhausted, deadline_missed, peer_gone };

const Conn = struct {
    id: u32,
    fd: i32,
    phase: Phase = .reading,
    /// Work units the request body may still spend. Caller-pays: admitted
    /// once, never topped up.
    budget: i64 = work_budget,
    /// Separate grant, only spendable during `.cleanup`.
    reserve: i64 = cleanup_reserve,
    deadline_ms: i64,
    /// POLL.IN / POLL.OUT while parked, 0 when runnable without I/O.
    want: i16,
    ending: Ending = .ok,
    work_left: i64 = 0,
    spent: i64 = 0,
    in_len: usize = 0,
    in: [512]u8 = undefined,
    out_len: usize = 0,
    out_sent: usize = 0,
    out: [256]u8 = undefined,
    sink: u64 = 0,
    keep_alive: bool = true,
};

const Kernel = struct {
    listener: i32,
    conns: [max_conns]?Conn = @splat(null),
    next_id: u32 = 1,
    rr: usize = 0,
    steps: u64 = 0,
    slept_ms: i64 = 0,
    accepted: u64 = 0,
    finished: u64 = 0,
    served: u64 = 0,
    polls: u64 = 0,
    poll_fds_total: u64 = 0,
    quiet: bool = false,

    // --------------------------------------------------------------- kernel

    /// One scheduler iteration. Blocks in `poll` iff nothing is runnable.
    fn step(k: *Kernel) void {
        k.steps += 1;

        var fds: [max_conns + 1]posix.pollfd = undefined;
        var slot: [max_conns + 1]usize = undefined;
        var n: usize = 0;

        const room = k.freeSlot() != null;
        if (room) {
            fds[n] = .{ .fd = k.listener, .events = posix.POLL.IN, .revents = 0 };
            slot[n] = std.math.maxInt(usize); // listener sentinel
            n += 1;
        }

        var runnable = false;
        var soonest: i64 = std.math.maxInt(i64);
        for (&k.conns, 0..) |*maybe, i| {
            const c = &(maybe.* orelse continue);
            if (c.want == 0) {
                runnable = true;
            } else {
                fds[n] = .{ .fd = c.fd, .events = c.want, .revents = 0 };
                slot[n] = i;
                n += 1;
                soonest = @min(soonest, c.deadline_ms);
            }
        }

        // The deadline IS the poll timeout. No watchdog, no deadline atomics,
        // no shutting a socket down to wake a parked read.
        const timeout: i32 = if (runnable)
            0
        else if (soonest == std.math.maxInt(i64))
            1000
        else
            @intCast(@max(0, @min(1000, soonest - nowMs())));

        const before = nowMs();
        k.polls += 1;
        k.poll_fds_total += n;
        const ready = posix.poll(fds[0..n], timeout) catch 0;
        k.slept_ms += nowMs() - before; // actual time blocked, not requested

        if (ready > 0) {
            for (fds[0..n], slot[0..n]) |pfd, idx| {
                if (pfd.revents == 0) continue;
                if (idx == std.math.maxInt(usize)) {
                    var burst: usize = 0;
                    while (burst < 256) : (burst += 1) if (!k.doAccept()) break;
                } else if (k.conns[idx]) |*c| {
                    c.want = 0; // I/O ready: runnable again
                }
            }
        }

        // Expire deadlines. A missed deadline is a transition, not a kill:
        // the connection lands in cleanup with its reserve intact.
        const now = nowMs();
        for (&k.conns) |*maybe| {
            const c = &(maybe.* orelse continue);
            if (c.phase != .cleanup and c.phase != .done and now >= c.deadline_ms) {
                enterCleanup(c, .deadline_missed);
            }
        }

        // Round-robin one runnable connection.
        var tries: usize = 0;
        while (tries < max_conns) : (tries += 1) {
            const i = (k.rr + tries) % max_conns;
            const c = &(k.conns[i] orelse continue);
            if (c.want != 0) continue;
            k.rr = (i + 1) % max_conns;
            k.run(i);
            return;
        }
    }

    fn freeSlot(k: *Kernel) ?usize {
        for (&k.conns, 0..) |*c, i| if (c.* == null) return i;
        return null;
    }

    fn doAccept(k: *Kernel) bool {
        const i = k.freeSlot() orelse return false;
        const rc = linux.accept4(k.listener, null, null, linux.SOCK.NONBLOCK);
        if (sysErr(rc)) return false;
        const fd: i32 = @intCast(rc);
        k.conns[i] = .{
            .id = k.next_id,
            .fd = fd,
            .deadline_ms = nowMs() + idle_deadline_ms,
            .want = posix.POLL.IN,
        };
        k.next_id += 1;
        k.accepted += 1;
        if (!k.quiet) log("conn {d}: accepted (budget {d}, reserve {d})", .{ k.conns[i].?.id, work_budget, cleanup_reserve });
        return true;
    }

    // ----------------------------------------------------------- one quantum

    fn run(k: *Kernel, i: usize) void {
        const c = &(k.conns[i] orelse return);
        switch (c.phase) {
            .reading => stepReading(c),
            .working => stepWorking(c),
            .writing, .cleanup => stepWriting(c),
            .done => {},
        }
        if (c.phase == .done and c.ending == .ok and c.keep_alive) {
            k.served += 1;
            c.phase = .reading;
            c.budget = work_budget;
            c.reserve = cleanup_reserve;
            c.spent = 0;
            c.in_len = 0;
            c.out_len = 0;
            c.out_sent = 0;
            c.deadline_ms = nowMs() + idle_deadline_ms;
            c.want = posix.POLL.IN;
            return;
        }
        if (c.phase == .done) {
            k.served += 1;
            _ = linux.close(c.fd);
            k.finished += 1;
            if (!k.quiet) log("conn {d}: closed [{s}] spent={d}/{d} reserve_left={d}", .{
                c.id, @tagName(c.ending), c.spent, work_budget, c.reserve,
            });
            k.conns[i] = null;
        }
    }
};

// ------------------------------------------------------------------- phases

fn stepReading(c: *Conn) void {
    const rc = linux.read(c.fd, c.in[c.in_len..].ptr, c.in.len - c.in_len);
    if (sysErr(rc)) return enterCleanup(c, .peer_gone);
    const got: usize = rc;
    if (got == 0) return enterCleanup(c, .peer_gone);
    c.in_len += got;

    const head = std.mem.indexOf(u8, c.in[0..c.in_len], "\r\n") orelse {
        c.want = posix.POLL.IN; // park again, deadline unchanged
        return;
    };
    // "GET /work/1234 HTTP/1.1" -> ask for 1234 units of work.
    const line = c.in[0..head];
    c.work_left = blk: {
        const p = std.mem.indexOf(u8, line, "/work/") orelse break :blk 100;
        var end = p + 6;
        while (end < line.len and line[end] >= '0' and line[end] <= '9') end += 1;
        break :blk std.fmt.parseInt(i64, line[p + 6 .. end], 10) catch 100;
    };

    c.phase = .working;
    c.want = 0;
}

fn stepWorking(c: *Conn) void {
    // Charge before doing the work, and only what the budget still allows.
    const charge = @min(quantum_units, c.work_left);
    if (charge > c.budget) {
        // The body's budget is gone. The reserve is untouched — that is the
        // whole point of keeping it in a separate field.
        return enterCleanup(c, .budget_exhausted);
    }
    c.budget -= charge;
    c.spent += charge;
    c.work_left -= charge;

    var j: i64 = 0;
    while (j < charge * 200) : (j += 1) c.sink +%= @intCast(j);

    if (c.work_left > 0) {
        c.want = 0; // still runnable; yields to round-robin
        return;
    }
    c.out_len = (std.fmt.bufPrint(&c.out,
        "HTTP/1.1 200 OK\r\nContent-Length: 24\r\n\r\ndone, spent {d:>5} units\n", .{c.spent}) catch return enterCleanup(c, .peer_gone)).len;
    c.phase = .writing;
    c.want = posix.POLL.OUT;
}

/// The unwind. Reached from every ending, including the happy one, and always
/// funded by `reserve` rather than `budget`.
fn enterCleanup(c: *Conn, why: Ending) void {
    c.ending = why;
    if (why == .peer_gone) {
        c.phase = .done;
        return;
    }
    c.phase = .cleanup;
    c.want = posix.POLL.OUT;
    c.reserve -= 10; // unwind costs something, and it comes from the reserve
    c.deadline_ms = nowMs() + 500; // short grace to flush
    const body = switch (why) {
        .budget_exhausted => "503 budget exhausted\n",
        .deadline_missed => "408 deadline missed\n",
        else => "500\n",
    };
    const status = if (why == .budget_exhausted) "503 Service Unavailable" else "408 Request Timeout";
    c.out_sent = 0;
    c.out_len = (std.fmt.bufPrint(&c.out, "HTTP/1.1 {s}\r\nContent-Length: {d}\r\n\r\n{s}", .{ status, body.len, body }) catch {
        c.phase = .done;
        return;
    }).len;

}

fn stepWriting(c: *Conn) void {
    const rc = linux.write(c.fd, c.out[c.out_sent..].ptr, c.out_len - c.out_sent);
    if (sysErr(rc)) {
        c.phase = .done;
        return;
    }
    c.out_sent += rc;
    if (c.out_sent < c.out_len) {
        c.want = posix.POLL.OUT;
        return;
    }
    c.phase = .done;
}

// --------------------------------------------------------------------- main

fn log(comptime fmt: []const u8, args: anytype) void {
    std.debug.print("  " ++ fmt ++ "\n", args);
}

pub fn main(init: std.process.Init.Minimal) !void {
    var argv: [8][]const u8 = undefined;
    var argc: usize = 0;
    for (init.args.vector) |a| { if (argc == 8) break; argv[argc] = std.mem.span(a); argc += 1; }
    const arg_port: u16 = if (argc > 1) (std.fmt.parseInt(u16, argv[1], 10) catch 0) else 0;
    const arg_secs: i64 = if (argc > 2) (std.fmt.parseInt(i64, argv[2], 10) catch 12) else 12;
    const arg_quiet: bool = argc > 3;

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
    const port = std.mem.bigToNative(u16, addr.port);

    var k: Kernel = .{ .listener = sock, .quiet = arg_quiet };
    std.debug.print("listening on 127.0.0.1:{d}\n", .{port});
    std.debug.print("budget={d} reserve={d} quantum={d} idle_deadline={d}ms max_conns={d}\n\n", .{
        work_budget, cleanup_reserve, quantum_units, idle_deadline_ms, max_conns,
    });

    const stop = nowMs() + arg_secs * 1000;
    while (nowMs() < stop) k.step();

    std.debug.print(
        \\
        \\steps={d} accepted={d} served={d} polls={d} avg_pollfds={d} asleep={d}ms
        \\
    , .{ k.steps, k.accepted, k.served, k.polls, if (k.polls > 0) k.poll_fds_total / k.polls else 0, k.slept_ms });
}
