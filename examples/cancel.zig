//! Cancellation on its own, with nothing else in the way.
//!
//! No sockets, no reactor, no protocol. Four tasks race to find an answer, the
//! first one to get there tells a supervisor, and the supervisor cancels the
//! rest. That is the oldest use for cancellation there is, and it is enough to
//! show every part of the interface.
//!
//! One of the four is written wrong on purpose. It never asks whether it has
//! been cancelled. Watch what happens to it, because that is the part that
//! separates this from a flag: the scheduler does not need the task to
//! cooperate, since it holds what the task needs in order to proceed.
//!
//! Run it with `zig build cancel`.

const std = @import("std");
const budgie = @import("budgie");

const sched = budgie.sched;
const quota = budgie.quota;
const sys = budgie.sys;
const TaskId = sched.TaskId;

/// The two values the program owns, same as `examples/echo.zig`.
var s: sched.Sched = .{};
var q: quota.Tree = .{};

const supervisor: TaskId = 0;
const n_searchers = 4;

/// Units charged per turn, and the per-task ceiling. Sized so nothing here
/// runs out of budget on its own; the only thing that takes a budget away in
/// this program is a cancel.
const quantum: i64 = 100;
const work_cap: i64 = 100_000;

/// The unwind is paid for out of a different number, which `charge` has no
/// path into. That separation is the whole reason a cancelled task can still
/// clean up after its budget is gone.
const reserve: i64 = 50;
const unwind_cost: i64 = 10;

/// How long the loop tolerates a task that was cancelled and did not go away.
/// Without it, the heedless searcher below spins until the machine is turned
/// off, which is exactly the point it is here to make.
const patience = 20_000;

/// Application state, one entry per task, indexed by task id. The scheduler
/// holds none of this and has no idea what a searcher is.
const Searcher = struct {
    name: []const u8 = "",
    /// Work still to do before this one has an answer.
    work_left: i64 = 0,
    spent: i64 = 0,
    /// Stands in for a buffer, a descriptor, anything the unwind has to give
    /// back. Counting these is how the leak at the end becomes visible.
    holds_slot: bool = false,
    /// Parks on a long deadline instead of working, so there is one task that
    /// is waiting rather than running when the cancel arrives.
    waits: bool = false,
    /// Never asks whether it has been cancelled.
    heedless: bool = false,
    ending: Ending = .running,
    futile_turns: usize = 0,
};

const Ending = enum { running, found, unwound };

var searchers: [n_searchers + 1]Searcher = @splat(.{});

/// One token per searcher, minted at admission. A token is a right to cancel
/// one specific instance of one specific task, and it is the supervisor that
/// holds them. Nothing gives a task the right to cancel itself.
var toks: [n_searchers + 1]sched.CancelTok = @splat(.{ .task = 0, .gen = 0 });

var slots_held: usize = 0;
var winner: ?TaskId = null;
var cancel_sent_ms: i64 = 0;

fn nowMs() i64 {
    return @divTrunc(budgie.clock.monotonicNs(), 1_000_000);
}

fn takeSlot(w: *Searcher) void {
    w.holds_slot = true;
    slots_held += 1;
}

fn dropSlot(w: *Searcher) void {
    if (!w.holds_slot) return;
    w.holds_slot = false;
    slots_held -= 1;
}

// ----------------------------------------------------------------- the tasks

fn stepSearcher(t: TaskId) void {
    const w = &searchers[t];

    // Ask, at the top, every turn. One load, one place to look.
    //
    // There are two ways to learn about a cancellation here and they are not
    // interchangeable. `reasonFor(t)` says why this task was woken, and that
    // is a one-shot fact: run once, make yourself runnable again for any other
    // reason, and `.cancelled` is gone. Writing the check that way reproduces
    // precisely the bug this design exists to avoid.
    //
    // `isCancelled(t)` is the state. It stays true, reading it does not clear
    // it, and there is exactly one of it however deep the task's own call
    // stack goes. That is the one to build on. The wake reason is for the
    // other question, which is why you woke up earlier than you expected to.
    if (!w.heedless and s.isCancelled(t)) return unwind(t, "cancelled");

    if (w.waits) {
        // Parked five seconds out. If a cancel could not reach a task that is
        // waiting rather than running, this program would take five seconds to
        // finish. It takes a few milliseconds, and the report says which.
        s.arm(t, nowMs() + 5_000);
        return;
    }

    if (!s.charge(t, &q, quantum)) {
        // A cancel zeroed the budget and the ceiling, so there is nothing left
        // to charge against and nothing that can top it up. For the heedless
        // searcher this is the only thing standing between it and running
        // forever, and it never learns why. It cannot proceed, and it also
        // does not stop, which is the honest limit of a cooperative runtime
        // and worth seeing rather than glossing.
        w.futile_turns += 1;
        return s.makeRunnable(t, .spawn);
    }

    w.spent += quantum;
    w.work_left -= quantum;
    if (w.work_left > 0) return s.makeRunnable(t, .spawn);

    w.ending = .found;
    dropSlot(w);
    if (winner == null) winner = t;
    std.debug.print("  {s:<9} found an answer after {d} units\n", .{ w.name, w.spent });
    s.release(t);
    s.makeRunnable(supervisor, .spawn);
}

fn unwind(t: TaskId, why: []const u8) void {
    const w = &searchers[t];

    // The body budget is zero by now. This still works, because the reserve is
    // a separate number and `charge` has no path into it. Cleanup is funded by
    // construction rather than by a runtime check that could fail at the one
    // moment it matters.
    s.chargeReserve(t, unwind_cost);
    dropSlot(w);
    w.ending = .unwound;

    std.debug.print("  {s:<9} {s} after {d} units, {d}ms later, reserve left {d}\n", .{
        w.name, why, w.spent, nowMs() - cancel_sent_ms, s.reserve[t],
    });
    s.release(t);
    s.makeRunnable(supervisor, .spawn);
}

fn stepSupervisor() void {
    const win = winner orelse return;
    if (cancel_sent_ms != 0) return;
    cancel_sent_ms = nowMs();

    std.debug.print("\n{s} won. Cancelling the others.\n", .{searchers[win].name});
    for (1..n_searchers + 1) |i| {
        const t: TaskId = @intCast(i);
        // The winner has already released its slot, so its token names a
        // generation that no longer exists. That is not an error and not a
        // kill of whoever inherits the slot next. It is a no-op that says so.
        const taken = s.cancel(toks[t]);
        std.debug.print("  cancel {s:<9} -> {s}\n", .{
            searchers[t].name,
            if (taken) "taken" else "stale, ignored",
        });
    }
    std.debug.print("\n", .{});
}

fn step(t: TaskId) void {
    if (t == supervisor) return stepSupervisor();
    stepSearcher(t);
}

// ------------------------------------------------------------------ the loop

pub fn main() !void {
    q.define(0, quota.none, quota.unlimited, .periodic, 100, "root");

    // Above the searchers, so a cancel goes out promptly.
    _ = s.admit(supervisor, .{ .prio = 0, .quota = 0, .cap = work_cap, .reserve = reserve });

    const plan = [n_searchers]Searcher{
        .{ .name = "quick", .work_left = 300 },
        .{ .name = "slow", .work_left = 500_000 },
        .{ .name = "waiting", .work_left = 400, .waits = true },
        .{ .name = "heedless", .work_left = 500_000, .heedless = true },
    };

    for (plan, 1..) |p, i| {
        const t: TaskId = @intCast(i);
        searchers[t] = p;
        takeSlot(&searchers[t]);
        // The token comes back from admission, which is the only place it
        // can be minted. There is no longer a way to hold one for a task that
        // was never admitted.
        toks[t] = s.admit(t, .{ .prio = 1, .quota = 0, .cap = work_cap, .reserve = reserve });
    }

    std.debug.print("four searchers, {d} slots held\n\n", .{slots_held});

    const started = nowMs();
    var turns: usize = 0;
    while (turns < patience) : (turns += 1) {
        // Bounded, not drain-until-empty. The obvious loop is
        //
        //     while (s.popRunnable()) |t| step(t);
        //
        // and it never returns here, because a task that makes itself runnable
        // again is still runnable when you look. Two of these four do that
        // forever. `app/server.zig` has the same bound for the same reason and
        // calls it `bounded_drain`. Worth knowing that the cost of a task that
        // will not yield is paid by whoever wrote the loop, not by the task.
        var dispatched: usize = 0;
        while (dispatched < 64) : (dispatched += 1) {
            const t = s.popRunnable() orelse break;
            step(t);
        }
        s.expire(nowMs());
        if (!s.anyRunnable()) {
            const next = s.timeoutMs(nowMs()) orelse break;
            sys.sleepMs(@intCast(@max(1, next)));
        }
        if (settled()) break;
    }

    report(nowMs() - started, turns);
}

fn settled() bool {
    for (searchers[1..]) |w| if (w.ending == .running) return false;
    return true;
}

fn report(elapsed_ms: i64, turns: usize) void {
    if (settled()) {
        std.debug.print("all four settled in {d}ms over {d} turns\n", .{ elapsed_ms, turns });
    } else {
        std.debug.print("gave up after {d}ms and {d} turns, out of patience\n", .{ elapsed_ms, turns });
    }
    std.debug.print("  the waiting searcher was parked 5000ms out, so the first number is the point\n", .{});
    std.debug.print("cancels: {d} taken, {d} stale\n", .{ s.cancels, s.cancels_stale });

    // The part the runtime cannot answer.
    //
    // Reading two of the scheduler's arrays by hand, because there is no call
    // that asks "which tasks were cancelled and are still here". A task that
    // ignores its cancellation cannot do any work, which is most of what you
    // want, but it also never gives its slot back, and nothing anywhere
    // notices. Making that visible is the open design question this example
    // was written to put in front of us.
    var stuck: usize = 0;
    for (1..n_searchers + 1) |i| {
        const t: TaskId = @intCast(i);
        if (!s.live[t] or !s.cancelled[t]) continue;
        stuck += 1;
        std.debug.print(
            "  {s:<9} was cancelled and is still here: {d} attempts, all refused, {d} units done, slot still held\n",
            .{ searchers[t].name, searchers[t].futile_turns, searchers[t].spent },
        );
    }

    std.debug.print("slots still held: {d} (want 0)\n", .{slots_held});
    if (stuck != 0) std.debug.print("tasks cancelled but not unwound: {d}\n", .{stuck});
}
