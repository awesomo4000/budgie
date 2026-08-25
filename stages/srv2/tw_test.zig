const std = @import("std");
const S = @import("sched.zig");
pub fn main() !void {
    var s: S.Sched = .{};
    // 1. basic fire ordering
    for (1..6) |i| { s.live[@intCast(i)] = true; s.admit(@intCast(i), 100, 10); }
    while (s.popRunnable()) |_| {}
    const t0: i64 = 1_000_000;
    s.cur = t0;
    s.arm(3, t0 + 30); s.arm(1, t0 + 10); s.arm(5, t0 + 5000); s.arm(2, t0 + 20); s.arm(4, t0 + 40);
    std.debug.print("armed={d} timeout@t0={any}ms (expect 10)\n", .{ s.armed, s.timeoutMs(t0) });
    s.expire(t0 + 25);
    var order: [8]S.TaskId = undefined; var n: usize = 0;
    while (s.popRunnable()) |t| { order[n] = t; n += 1; }
    std.debug.print("fired by t0+25: {any} (expect 1,2)\n", .{order[0..n]});
    // 2. rearm leaves nothing stale
    s.arm(3, t0 + 100); s.arm(3, t0 + 200); s.arm(3, t0 + 300);
    s.expire(t0 + 250);
    n = 0; while (s.popRunnable()) |t| { order[n] = t; n += 1; }
    std.debug.print("after triple-rearm to +300, fired by +250: {any} (expect 4 only)\n", .{order[0..n]});
    s.expire(t0 + 350);
    n = 0; while (s.popRunnable()) |t| { order[n] = t; n += 1; }
    std.debug.print("fired by +350: {any} (expect 3, exactly once)\n", .{order[0..n]});
    // 3. disarm
    s.arm(1, t0 + 400); s.disarm(1);
    s.expire(t0 + 500);
    n = 0; while (s.popRunnable()) |t| { order[n] = t; n += 1; }
    std.debug.print("disarmed task fired: {any} (expect none)\n", .{order[0..n]});
    // 4. overflow beyond one revolution (task 5 at +5000 > 4096)
    s.expire(t0 + 5100);
    n = 0; while (s.popRunnable()) |t| { order[n] = t; n += 1; }
    std.debug.print("overflow task at +5000 fired by +5100: {any} (expect 5)\n", .{order[0..n]});
    std.debug.print("armed now={d} fires={d} rearms={d}\n", .{ s.armed, s.fires, s.rearms });
}
