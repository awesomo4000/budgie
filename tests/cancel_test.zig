const quota = @import("budgie").quota;
const std = @import("std");
const S = @import("budgie").sched;

// Reproduces the bug the fix addresses, at the scheduler level.
var q: quota.Tree = .{};

pub fn main() !void {
    q.define(0, quota.none, quota.unlimited, .periodic, 100, "root");
    var s: S.Sched = .{};
    s.cur = 1000;

    // --- 1. cancel is prompt: a parked task becomes runnable immediately
    // The token comes back from admission; there is no other way to get one.
    const tok = s.admit(1, .{ .prio = 1, .quota = 0, .cap = 1000, .reserve = 50 });
    while (s.popRunnable()) |_| {}          // consume the .spawn wake
    s.arm(1, 1_000_000);                     // parked with a far-off deadline
    std.debug.print("before cancel: runnable={}\n", .{s.anyRunnable()});
    _ = s.cancel(tok);
    const woke = s.popRunnable().?;
    std.debug.print("after cancel: woke task {d} reason={s} budget={d} reserve={d}\n",
        .{ woke, @tagName(s.reasonFor(woke)), s.budget[1], s.reserve[1] });

    // --- 2. safety: body work impossible, cleanup still funded
    std.debug.print("charge(1,1) after cancel = {} (expect false)\n", .{s.charge(1, &q, 1)});
    s.chargeReserve(1, 10);
    std.debug.print("reserve after unwind charge = {d} (expect 40)\n", .{s.reserve[1]});

    // --- 3. sticky: survives a refund attempt (keep-alive path)
    s.renewCap(1, 1000);
    s.setReserve(1, 50);
    std.debug.print("budget after refund attempt = {d} (expect 0)\n", .{s.budget[1]});

    // --- 4. idempotent
    std.debug.print("second cancel = {} (expect true, no double-wake)\n", .{s.cancel(tok)});
    std.debug.print("extra wakes queued = {}\n", .{s.anyRunnable()});

    // --- 5. generational: token for a recycled slot is a no-op
    s.release(1);
    _ = s.admit(1, .{ .prio = 1, .quota = 0, .cap = 1000, .reserve = 50 }); // different connection, same slot
    while (s.popRunnable()) |_| {}
    const ok = s.cancel(tok);                // stale token
    std.debug.print("stale token cancel = {} (expect false)  new task cancelled={} (expect false)\n",
        .{ ok, s.isCancelled(1) });
    std.debug.print("cancels={d} stale={d}\n", .{ s.cancels, s.cancels_stale });
}
