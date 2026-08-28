//! The same race as `examples/cancel.zig`, with the cancellation check moved
//! out of the task and into a dispatcher the task author never edits.
//!
//! Read the two `step` functions next to each other. That is the whole
//! comparison. In `cancel.zig` a searcher begins with
//!
//!     if (s.isCancelled(t)) return unwind(t, "cancelled");
//!
//! and forgetting that line is a bug nobody can see, because it is an absence.
//! Here the searcher begins with the work, because it is not the searcher's
//! job to ask. `Supervised(T)` asks, once, on the way in.
//!
//! What that buys, precisely:
//!
//!   - You cannot forget to ask, because you do not ask.
//!   - You cannot ask the wrong thing. There is no choice between
//!     `isCancelled` and the `.cancelled` wake reason at the call site,
//!     because there is no call site.
//!   - You cannot ship a task with no unwind path. `Supervised` will not
//!     compile against a type that has no `unwind`.
//!
//! What it does not buy, and this is the part worth watching: it cannot make
//! your `unwind` do anything. `negligent` below has one, and it is empty, and
//! the runtime calls it faithfully. The bug did not go away. It moved from an
//! absence to a presence, which is the trade this file exists to show. An
//! empty function is visible in a diff and a missing line is not.
//!
//! Run it with `zig build cancel-supervised`, and `zig build cancel` for the
//! hand-written one.

const std = @import("std");
const budgie = @import("budgie");

const sched = budgie.sched;
const quota = budgie.quota;
const sys = budgie.sys;
const TaskId = sched.TaskId;

var s: sched.Sched = .{};
var q: quota.Tree = .{};

const supervisor: TaskId = 0;
const n_searchers = 4;

const quantum: i64 = 100;
const work_cap: i64 = 100_000;
const reserve: i64 = 50;
const unwind_cost: i64 = 10;
const patience = 20_000;

// ------------------------------------------------------------ the dispatcher

/// Wraps a task type so that a fault is handled before the task runs.
///
/// This is application code, deliberately. The scheduler reports the fault and
/// stops there; it has no idea what a task is for, what it holds, or what
/// unwinding one costs. Where the handling sits is the programmer's call, and
/// this file makes one choice out of several reasonable ones. It could equally
/// be a supervisor task with its own budget and priority, which would cost a
/// dispatch per fault and buy the ability to meter the handling itself.
///
/// The comptime checks are the only mechanised part of the whole idea, and
/// they are worth exactly what they say: a type without an `unwind` will not
/// compile. Whether that `unwind` releases anything is beyond what any
/// language can promise.
fn Supervised(comptime T: type) type {
    comptime {
        if (!@hasDecl(T, "step")) @compileError(@typeName(T) ++ " needs a `step`");
        if (!@hasDecl(T, "unwind")) @compileError(
            @typeName(T) ++ " needs an `unwind`: every task that can be" ++
                " cancelled needs somewhere for the cancellation to go",
        );
    }
    return struct {
        pub fn run(t: TaskId) void {
            // The one place the question is asked, in code no task author
            // edits. `faultOf` returns an optional enum rather than a bool so
            // that the cause has to be captured rather than discarded, and so
            // that a second variant would break every handler that has to
            // start deciding between them.
            if (s.faultOf(t)) |f| return T.unwind(t, f);
            T.step(t);
        }
    };
}

// ---------------------------------------------------------------- task state

const Searcher = struct {
    name: []const u8 = "",
    work_left: i64 = 0,
    spent: i64 = 0,
    holds_slot: bool = false,
    waits: bool = false,
    /// Has an `unwind` that does nothing. The compiler is satisfied and the
    /// slot still leaks.
    negligent: bool = false,
    ending: Ending = .running,
    unwinds_called: usize = 0,
};

const Ending = enum { running, found, unwound };

var searchers: [n_searchers + 1]Searcher = @splat(.{});
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

// ------------------------------------------------------------- the behaviour

const SearcherTask = struct {
    /// No cancellation check. Compare with `stepSearcher` in `cancel.zig`,
    /// which opens with one and would be silently broken without it.
    pub fn step(t: TaskId) void {
        const w = &searchers[t];

        if (w.waits) {
            s.arm(t, nowMs() + 5_000);
            return;
        }

        if (!s.charge(t, &q, quantum)) return; // nothing left to spend
        w.spent += quantum;
        w.work_left -= quantum;
        if (w.work_left > 0) return s.makeRunnable(t, .spawn);

        w.ending = .found;
        dropSlot(w);
        if (winner == null) winner = t;
        std.debug.print("  {s:<10} found an answer after {d} units\n", .{ w.name, w.spent });
        s.release(t);
        s.makeRunnable(supervisor, .spawn);
    }

    /// Called by the dispatcher, never by the task. The reserve funds this and
    /// the body budget cannot reach it, so it works with `budget` at zero.
    pub fn unwind(t: TaskId, f: sched.Fault) void {
        const w = &searchers[t];
        w.unwinds_called += 1;

        // Written on purpose as the thing a careless author would write. It
        // compiles, the runtime calls it, and it gives nothing back.
        if (w.negligent) return;

        s.chargeReserve(t, unwind_cost);
        dropSlot(w);
        w.ending = .unwound;
        std.debug.print("  {s:<10} {s} after {d} units, {d}ms later, reserve left {d}\n", .{
            w.name, @tagName(f), w.spent, nowMs() - cancel_sent_ms, s.reserve[t],
        });
        s.release(t);
        s.makeRunnable(supervisor, .spawn);
    }
};

const Search = Supervised(SearcherTask);

// ------------------------------------------------------------------ the race

fn stepSupervisor() void {
    const win = winner orelse return;
    if (cancel_sent_ms != 0) return;
    cancel_sent_ms = nowMs();

    std.debug.print("\n{s} won. Cancelling the others.\n", .{searchers[win].name});
    for (1..n_searchers + 1) |i| {
        const t: TaskId = @intCast(i);
        const taken = s.cancel(toks[t]);
        std.debug.print("  cancel {s:<10} -> {s}\n", .{
            searchers[t].name,
            if (taken) "taken" else "stale, ignored",
        });
    }
    std.debug.print("\n", .{});
}

fn run(t: TaskId) void {
    if (t == supervisor) return stepSupervisor();
    Search.run(t);
}

pub fn main() !void {
    q.define(0, quota.none, quota.unlimited, .periodic, 100, "root");

    _ = s.admit(supervisor, .{ .prio = 0, .quota = 0, .cap = work_cap, .reserve = reserve });

    const plan = [n_searchers]Searcher{
        .{ .name = "quick", .work_left = 300 },
        .{ .name = "slow", .work_left = 500_000 },
        .{ .name = "waiting", .work_left = 400, .waits = true },
        .{ .name = "negligent", .work_left = 500_000, .negligent = true },
    };

    for (plan, 1..) |p, i| {
        const t: TaskId = @intCast(i);
        searchers[t] = p;
        takeSlot(&searchers[t]);
        toks[t] = s.admit(t, .{ .prio = 1, .quota = 0, .cap = work_cap, .reserve = reserve });
    }

    std.debug.print("four searchers, {d} slots held\n\n", .{slots_held});

    const started = nowMs();
    var turns: usize = 0;
    var why: Exit = .patience;
    while (turns < patience) : (turns += 1) {
        var dispatched: usize = 0;
        while (dispatched < 64) : (dispatched += 1) {
            const t = s.popRunnable() orelse break;
            run(t);
        }
        s.expire(nowMs());
        if (settled()) {
            why = .settled;
            break;
        }
        if (!s.anyRunnable()) {
            // Nothing to run and nothing armed. The loop has no more work it
            // could possibly do, which is not the same as the work being done.
            const next = s.timeoutMs(nowMs()) orelse {
                why = .quiescent;
                break;
            };
            sys.sleepMs(@intCast(@max(1, next)));
        }
    }

    report(why, nowMs() - started, turns);
}

/// Why the loop stopped. Worth distinguishing, because two of these three
/// look like success from outside.
const Exit = enum { settled, quiescent, patience };

fn settled() bool {
    for (searchers[1..]) |w| if (w.ending == .running) return false;
    return true;
}

fn report(why: Exit, elapsed_ms: i64, turns: usize) void {
    switch (why) {
        .settled => std.debug.print("all four settled in {d}ms over {d} turns\n", .{ elapsed_ms, turns }),
        .quiescent => std.debug.print(
            "went quiescent after {d}ms and {d} turns: nothing runnable, nothing armed\n",
            .{ elapsed_ms, turns },
        ),
        .patience => std.debug.print("gave up after {d}ms and {d} turns, out of patience\n", .{ elapsed_ms, turns }),
    }
    std.debug.print("cancels: {d} taken, {d} stale\n", .{ s.cancels, s.cancels_stale });

    var stuck: usize = 0;
    for (1..n_searchers + 1) |i| {
        const t: TaskId = @intCast(i);
        if (!s.live[t] or !s.cancelled[t]) continue;
        stuck += 1;
        std.debug.print(
            "  {s:<10} cancelled and still here: unwind ran {d} time(s) and released nothing\n",
            .{ searchers[t].name, searchers[t].unwinds_called },
        );
    }

    std.debug.print("slots still held: {d} (want 0)\n", .{slots_held});
    if (stuck != 0) {
        std.debug.print("tasks cancelled but not unwound: {d}\n", .{stuck});
        std.debug.print(
            "\nNote the difference from `zig build cancel`. There the runtime asked\n" ++
                "the task a million times and got nowhere, because the task never\n" ++
                "checked, and the loop burned its whole patience. Here the runtime\n" ++
                "called `unwind` once, the task did nothing, and the loop went\n" ++
                "QUIESCENT: nothing runnable, nothing armed, a slot still held. From\n" ++
                "outside that is indistinguishable from having finished.\n" ++
                "\nLouder in the source, quieter at runtime. Both halves are real, and\n" ++
                "the quiet half is why a runtime that can report its own cancelled and\n" ++
                "un-unwound tasks is worth more than it first appears.\n",
            .{},
        );
    }
}
