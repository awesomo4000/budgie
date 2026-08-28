//! The timer wheel, past the end of one revolution.
//!
//! The wheel is 4096 slots of 1ms, so anything further out than about four
//! seconds cannot be linked into a slot and parks on an overflow list instead.
//! Everything the servers arm by default is inside that range -- the idle
//! deadline is three seconds -- so the overflow path ran essentially never,
//! and it was wrong.
//!
//! `arm` tested "already armed" by asking whether the task had a slot. An
//! overflow entry has no slot. So re-arming one skipped the unlink and pushed
//! it onto the list again, and since the push does `t.next = s.overflow` while
//! `s.overflow` is already this task, the list closed into a ring pointing at
//! itself. Every later walk of it ran forever: the sweep in `expire`, the scan
//! in `timeoutMs`. A server with a longer idle deadline spun at 100% CPU with
//! a connection half-answered and no error anywhere.
//!
//! The checks below walk the list with a bound rather than to its end, because
//! the failure being guarded against is a walk that never ends, and a test that
//! hangs tells you less than one that fails.

const std = @import("std");
const S = @import("budgie").sched;

var failures: usize = 0;
var checks: usize = 0;

fn check(ok: bool, comptime name: []const u8, detail: anytype) void {
    checks += 1;
    if (ok) {
        std.debug.print("  ok    {s}\n", .{name});
    } else {
        failures += 1;
        std.debug.print("  FAIL  {s}  {any}\n", .{ name, detail });
    }
}

/// Length of the overflow list, giving up rather than looping if it is a ring.
/// Returns null when the walk did not terminate within the task limit.
fn overflowLen(s: *const S.Sched) ?usize {
    var n: usize = 0;
    var id = s.overflow;
    while (id != nil_id) : (id = s.timer[id].next) {
        n += 1;
        if (n > S.max_tasks) return null;
    }
    return n;
}

const nil_id: u32 = 0xffff_ffff;

/// Comfortably past `wheel_slots` (4096), so `arm` has to take the overflow
/// path rather than finding a slot.
const far: i64 = 10_000;

pub fn main() !void {
    var s: S.Sched = .{};
    s.cur = 1000;

    _ = s.admit(1, .{ .prio = 1, .quota = 0, .cap = 1000, .reserve = 50 });
    while (s.popRunnable()) |_| {}

    s.arm(1, s.cur + far);
    check(overflowLen(&s) != null and overflowLen(&s).? == 1, "a far deadline parks on the overflow list", .{
        .len = overflowLen(&s),
        .armed = s.armed,
    });

    // The re-arm. This is what the servers do on every keep-alive request.
    s.arm(1, s.cur + far + 2000);
    const after_rearm = overflowLen(&s);
    check(after_rearm != null, "re-arming an overflow entry does not close the list into a ring", .{});
    if (after_rearm == null) {
        // Everything below walks the list, directly or through the scheduler,
        // so carrying on would hang instead of reporting.
        std.debug.print("\n{d} checks, {d} failures (stopped: the overflow list is a ring)\n", .{ checks, failures });
        std.process.exit(1);
    }
    check(after_rearm.? == 1, "re-arming leaves one entry, not two", .{ .len = after_rearm });
    check(s.armed == 1, "re-arming does not double-count the armed total", .{ .armed = s.armed });

    const t = s.timeoutMs(s.cur);
    check(t != null and t.? == far + 2000, "the timeout reflects the second deadline, not the first", .{ .ms = t });

    // Still nothing due.
    s.expire(s.cur + 100);
    check(!s.anyRunnable(), "an overflow deadline does not fire early", .{});

    // Come due. Sweeping is what finally fires it.
    s.expire(1000 + far + 2000);
    const woke = s.popRunnable();
    check(woke != null and woke.? == 1, "an overflow deadline fires when it comes due", .{ .woke = woke });
    check((overflowLen(&s) orelse 99) == 0, "the fired entry leaves the list", .{ .len = overflowLen(&s) });
    check(s.armed == 0, "the armed total returns to zero", .{ .armed = s.armed });

    // Disarm, from overflow, without firing.
    s.arm(1, s.cur + far);
    s.disarm(1);
    check((overflowLen(&s) orelse 99) == 0, "disarming an overflow entry unlinks it", .{ .len = overflowLen(&s) });
    check(s.armed == 0, "disarming an overflow entry decrements the armed total", .{ .armed = s.armed });

    // Two tasks in overflow at once, and the one in the middle re-armed.
    _ = s.admit(2, .{ .prio = 1, .quota = 0, .cap = 1000, .reserve = 50 });
    _ = s.admit(3, .{ .prio = 1, .quota = 0, .cap = 1000, .reserve = 50 });
    while (s.popRunnable()) |_| {}
    s.arm(1, s.cur + far);
    s.arm(2, s.cur + far + 1);
    s.arm(3, s.cur + far + 2);
    s.arm(2, s.cur + far + 500); // the middle one, re-armed
    check((overflowLen(&s) orelse 99) == 3, "three armed, three on the list after a re-arm in the middle", .{
        .len = overflowLen(&s),
        .armed = s.armed,
    });

    // And a task that moves from overflow back inside one revolution.
    s.arm(1, s.cur + 100);
    check((overflowLen(&s) orelse 99) == 2, "arming closer takes the task off the overflow list", .{
        .len = overflowLen(&s),
    });
    check(s.timer[1].slot != @as(u32, 0xffff_ffff), "and puts it in a slot", .{ .slot = s.timer[1].slot });

    std.debug.print("\n{d} checks, {d} failures\n", .{ checks, failures });
    if (failures != 0) std.process.exit(1);
}
