//! What happens to a block the kernel is still holding, on purpose and in bulk.
//!
//! Windows only, because the hazard is. On Linux and macOS, disarming is a
//! syscall that is finished when it returns: the epoll entry is gone, the knote
//! is gone, and whatever they referred to is yours again. IOCP does not work
//! that way. An operation is posted with an `OVERLAPPED` block the kernel owns
//! from that moment, `CancelIoEx` asks it to stop rather than telling it to,
//! and the block stays the kernel's until its completion packet has been
//! dequeued. Freeing at cancellation time is a use-after-free that the kernel
//! commits on your behalf, into whatever the block became, at a time it picks.
//!
//! `backend_iocp.zig` answers that with a third state: a cancelled block is
//! `orphan`, not `free`, and only becomes free when its packet arrives. This
//! file is here because that answer is invisible in normal operation. The
//! server cancels rarely and the pool is large, so the whole mechanism can be
//! subtly wrong and every other test still passes. So: cancel constantly,
//! cancel in the worst order, and count.
//!
//! The backend is driven directly rather than through `Reactor`. The unit under
//! test is the pool, and going through the reactor would need a scheduler with
//! admitted tasks to say anything about it.
//!
//! Three shapes, because they leave the kernel holding different things:
//!
//!   - Cancel a read that will never complete. The plainest orphan. The peer
//!     sends nothing, so the packet only exists because cancellation made it.
//!   - Switch direction without disarming. The backend supersedes the old
//!     operation itself, which is the path a caller cannot see and so the one
//!     most likely to be missed.
//!   - Cancel a pending `AcceptEx`. The expensive orphan: it holds a socket the
//!     backend created, and closing that socket at cancellation time rather
//!     than at reclaim is a descriptor leak in the good case and worse in the
//!     bad one.

const std = @import("std");
const builtin = @import("builtin");
const budgie = @import("budgie");
const sys = budgie.sys;
const backend = budgie.reactor.backend;

var failed = false;

fn check(ok: bool, what: []const u8, detail: anytype) void {
    if (ok) {
        std.debug.print("  ok    {s}\n", .{what});
    } else {
        failed = true;
        std.debug.print("  FAIL  {s}  {any}\n", .{ what, detail });
    }
}

const pairs = 24;
const rounds = 400;

const Pair = struct { client: i32, server: i32 };

fn listenLoopback() !struct { fd: i32, port: u16 } {
    const rc = sys.tcpSocketNonblock();
    if (sys.sysErr(rc)) return error.Socket;
    const fd: i32 = @intCast(rc);
    sys.setReuseAddr(fd);
    var addr = sys.SockAddrIn{ .port = 0, .addr = sys.loopback };
    if (sys.sysErr(sys.bind(fd, &addr))) return error.Bind;
    if (sys.sysErr(sys.listen(fd, 64))) return error.Listen;
    var got: sys.SockAddrIn = undefined;
    if (sys.sysErr(sys.getsockname(fd, &got))) return error.Sockname;
    return .{ .fd = fd, .port = sys.netToHostPort(got.port) };
}

fn connectPair(listener: i32, port: u16) !Pair {
    const crc = sys.tcpSocket();
    if (sys.sysErr(crc)) return error.Socket;
    const client: i32 = @intCast(crc);
    var addr = sys.SockAddrIn{ .port = sys.hostToNetPort(port), .addr = sys.loopback };
    if (sys.sysErr(sys.connect(client, &addr))) return error.Connect;

    var tries: usize = 0;
    while (tries < 400) : (tries += 1) {
        const arc = sys.acceptNonblock(listener);
        if (!sys.sysErr(arc)) return .{ .client = client, .server = @intCast(arc) };
        sys.sleepMs(2);
    }
    return error.Accept;
}

/// Collect until nothing is outstanding, or until it is clear nothing more is
/// coming. Bounded, because a hang here would be indistinguishable from the
/// leak this is looking for.
fn drain(be: *backend.Backend) usize {
    var out: [256]u32 = undefined;
    var spins: usize = 0;
    while (be.orphans_held != 0 and spins < 500) : (spins += 1) {
        _ = be.wait(&out, 2);
    }
    return spins;
}

pub fn main() !void {
    if (comptime builtin.os.tag != .windows) {
        std.debug.print("iocp_orphan_test: not Windows, nothing to test\n", .{});
        return;
    }

    var be: backend.Backend = .{};
    try be.init();
    defer be.deinit();

    const l = try listenLoopback();
    defer sys.close(l.fd);

    var conns: [pairs]Pair = undefined;
    var opened: usize = 0;
    while (opened < pairs) : (opened += 1) {
        conns[opened] = connectPair(l.fd, l.port) catch break;
    }
    defer for (conns[0..opened]) |p| {
        sys.close(p.client);
        sys.close(p.server);
    };
    check(opened == pairs, "opened every connection", .{ .opened = opened, .want = pairs });
    if (opened == 0) return error.NoConnections;

    var out: [256]u32 = undefined;

    // Nothing is ever sent on these, so a read that is armed and cancelled has
    // no reason to complete except the cancellation itself. Every packet
    // counted from here is one the kernel owed us.
    var round: usize = 0;
    while (round < rounds) : (round += 1) {
        for (conns[0..opened], 0..) |p, i| {
            const t: u32 = @intCast(i);
            _ = be.arm(t, p.server, .read, false);
            be.disarm(t, p.server);
        }
        if (round % 8 == 0) _ = be.wait(&out, 0);
    }
    const after_reads = be.orphans_made;
    check(after_reads >= rounds * opened, "every cancelled read left a block held", .{
        .made = after_reads,
        .want_at_least = rounds * opened,
    });

    // Direction switch. No disarm: the backend has to notice the task is
    // already parked on a read and supersede it, which orphans the old block
    // without the caller ever asking for a cancellation.
    round = 0;
    while (round < rounds) : (round += 1) {
        for (conns[0..opened], 0..) |p, i| {
            const t: u32 = @intCast(i);
            _ = be.arm(t, p.server, .read, false);
            _ = be.arm(t, p.server, .write, true);
        }
        if (round % 8 == 0) _ = be.wait(&out, 0);
    }
    check(be.orphans_made > after_reads, "a direction switch orphans the old operation", .{
        .made = be.orphans_made - after_reads,
    });

    // Cancelled accepts. Each one holds a socket the backend made, and the
    // only correct time to close it is when the packet says the kernel has
    // finished with the block.
    const accept_task: u32 = @intCast(opened + 1);
    round = 0;
    while (round < rounds) : (round += 1) {
        _ = be.arm(accept_task, l.fd, .read, false);
        be.disarm(accept_task, l.fd);
        if (round % 8 == 0) _ = be.wait(&out, 0);
    }
    check(be.late_closes > 0, "a cancelled accept holds its socket until the packet lands", .{
        .closed_late = be.late_closes,
    });

    // Run the pool dry on purpose.
    //
    // Everything above collects every eighth round, so blocks come back faster
    // than they are taken and the pool never actually fills. That leaves the
    // one path that matters most when it is finally needed untested: what
    // happens when a task asks to be armed and every block is held. The answer
    // is meant to be that `allocOp` collects without blocking, on the reasoning
    // that a cancelled operation's packet is usually already sitting in the
    // queue, and tries once more. If that reasoning is wrong the backend
    // returns "not armed" and the task parks with nothing able to wake it.
    //
    // So: never collect, and cancel far more than the pool can hold.
    const before_drains = be.alloc_drains;
    const dry_rounds = (be.ops.len / opened) + 200;
    round = 0;
    while (round < dry_rounds) : (round += 1) {
        for (conns[0..opened], 0..) |p, i| {
            const t: u32 = @intCast(i);
            _ = be.arm(t, p.server, .read, false);
            be.disarm(t, p.server);
        }
    }
    check(be.alloc_drains > before_drains, "an empty pool recovers by collecting rather than failing", .{
        .drains = be.alloc_drains - before_drains,
        .fails = be.alloc_fails,
    });

    // Now let everything come back.
    for (conns[0..opened], 0..) |p, i| be.disarm(@intCast(i), p.server);
    const spins = drain(&be);

    check(be.orphans_held == 0, "the kernel gave every block back", .{
        .still_held = be.orphans_held,
        .spins = spins,
    });
    check(be.orphans_made == be.orphans_reclaimed, "made and reclaimed agree", .{
        .made = be.orphans_made,
        .reclaimed = be.orphans_reclaimed,
    });
    check(be.alloc_fails == 0, "no task ever failed to arm for want of a block", .{
        .fails = be.alloc_fails,
        .drains = be.alloc_drains,
    });
    if (be.check()) |v| {
        check(false, "the pool agrees with itself", .{ .violated = v.what });
    } else {
        check(true, "the pool agrees with itself", .{});
    }

    std.debug.print(
        "  {d} orphans made, {d} reclaimed, peak {d} of {d} blocks held at once\n" ++
            "  {d} accept sockets closed on reclaim, {d} recovery drains, {d} woken with no packet\n",
        .{
            be.orphans_made,  be.orphans_reclaimed, be.orphan_peak, be.ops.len,
            be.late_closes,   be.alloc_drains,      be.ready_now,
        },
    );

    std.debug.print("\n{s}\n", .{if (failed) "FAILED" else "iocp orphan pool: all checks passed"});
    if (failed) return error.ChecksFailed;
}
