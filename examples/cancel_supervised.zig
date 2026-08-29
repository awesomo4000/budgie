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
//! Tasks here also do not touch their own runnability. `step` and `unwind`
//! return a `Next`, and the dispatcher requeues, parks or releases. A function
//! that must return a value cannot forget to say what happens next, which
//! removes a second absence-shaped bug: omit the self-requeue in `cancel.zig`
//! and the task stops silently until a deadline reports a timeout that never
//! happened. Afterwards the only callers of `makeRunnable` are the reactor,
//! `expire` and `cancel`.
//!
//! Read the report at the end before deciding that is all upside.
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

/// What a task wants to happen next. Returned, never called.
///
/// Before this, a task with work left said so by calling
/// `s.makeRunnable(t, .spawn)` on itself, and the failure mode was an absence
/// again: omit the line and the connection stops, silently, until a deadline
/// reaps it and reports a timeout for something that never timed out. Nothing
/// in a diff to see.
///
/// A function that has to return a value cannot forget to say what happens
/// next. And afterwards the only callers of `makeRunnable` are the reactor,
/// `expire` and `cancel`, which is to say: only things outside the task decide
/// whether a task runs.
const Next = enum {
    /// More to do. Go behind whatever else is runnable and come back.
    yield,
    /// Waiting for something the task has already arranged: a timer it armed,
    /// or a descriptor a reactor is watching. Do not requeue.
    parked,
    /// Finished. The runtime may reclaim the task.
    done,
};

/// Wraps a task type so that a fault is handled before the task runs, and the
/// task's disposition is acted on after.
///
/// This is application code, deliberately. The scheduler reports the fault and
/// stops there; it has no idea what a task is for, what it holds, or what
/// unwinding one costs. Where the handling sits is the programmer's call.
///
/// The comptime checks are the only mechanised part, and they are worth what
/// they say: a type with no `unwind` will not compile, and neither will one
/// whose `step` forgets to report a disposition. Whether that `unwind` does
/// anything is beyond what any language can promise.
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
            // The one place the fault question is asked, in code no task
            // author edits. `faultOf` returns an optional enum rather than a
            // bool so the cause has to be captured rather than discarded, and
            // so a second variant would break every handler that then has to
            // choose between them.
            //
            // An unwind gets a disposition too, because unwinding is not
            // always one pass: a real server has a refusal to write and that
            // write can block.
            const next = if (s.faultOf(t)) |f| T.unwind(t, f) else T.step(t);
            switch (next) {
                .yield => s.makeRunnable(t, .spawn),
                .done => s.release(t),
                .parked => {
                    // "Waiting for something" is checkable, cheaply. A task
                    // that parks without arranging a wake never runs again,
                    // which is the same silent hang as forgetting to yield
                    // wearing different clothes. There is no reactor here, so
                    // an armed timer is the only arrangement available; a real
                    // one would ask the reactor too.
                    if (s.timer[t].slot == 0xffff_ffff) {
                        std.debug.print("  !! task {d} parked with nothing to wake it\n", .{t});
                        parked_nowhere += 1;
                    }
                },
            }
        }
    };
}

var parked_nowhere: usize = 0;

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
    /// No cancellation check, and no call into the scheduler about whether it
    /// should run again. Compare with `stepSearcher` in `cancel.zig`, which
    /// opens with the first and is peppered with the second.
    pub fn step(t: TaskId) Next {
        const w = &searchers[t];

        if (w.waits) {
            s.arm(t, nowMs() + 5_000);
            return .parked;
        }

        if (!s.charge(t, &q, quantum)) return .parked; // nothing left to spend
        w.spent += quantum;
        w.work_left -= quantum;
        if (w.work_left > 0) return .yield;

        w.ending = .found;
        dropSlot(w);
        if (winner == null) winner = t;
        std.debug.print("  {s:<10} found an answer after {d} units\n", .{ w.name, w.spent });
        s.makeRunnable(supervisor, .spawn); // waking ANOTHER task is not the same thing
        return .done;
    }

    /// Called by the dispatcher, never by the task. The reserve funds this and
    /// the body budget cannot reach it, so it works with `budget` at zero.
    pub fn unwind(t: TaskId, f: sched.Fault) Next {
        const w = &searchers[t];
        w.unwinds_called += 1;

        // Written on purpose as the thing a careless author would write. It
        // compiles, the runtime calls it, it reports itself finished, and it
        // gives nothing back.
        if (w.negligent) return .done;

        s.chargeReserve(t, unwind_cost);
        dropSlot(w);
        w.ending = .unwound;
        std.debug.print("  {s:<10} {s} after {d} units, {d}ms later, reserve left {d}\n", .{
            w.name, @tagName(f), w.spent, nowMs() - cancel_sent_ms, s.reserve[t],
        });
        s.makeRunnable(supervisor, .spawn);
        return .done;
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
    if (parked_nowhere != 0) std.debug.print("tasks that parked with nothing to wake them: {d}\n", .{parked_nowhere});

    // Who lied, and what it cost.
    var bad: usize = 0;
    for (1..n_searchers + 1) |i| {
        const t: TaskId = @intCast(i);
        const w = &searchers[t];
        if (!w.holds_slot) continue;
        bad += 1;
        std.debug.print(
            "  {s:<10} unwind ran {d} time(s), reported itself finished, and released nothing\n",
            .{ w.name, w.unwinds_called },
        );
    }
    std.debug.print("slots still held: {d} (want 0)\n", .{slots_held});

    if (bad == 0) return;
    std.debug.print(
        \\
        \\And here is what the disposition cost us, which is worth more than what
        \\it bought.
        \\
        \\In the hand-written version the negligent task stayed live and cancelled
        \\forever, so a scan of the scheduler could find it. Here it returned
        \\`.done`, the dispatcher believed it and called `release`, and the task
        \\slot came back clean. The runtime now cannot tell a good unwind from a
        \\bad one, because the only evidence it had was the task failing to go
        \\away, and we just gave the task a way to say it went away.
        \\
        \\So `.done` is not a fact, it is a claim. The leak above is real and the
        \\scheduler has no idea. That argues two things: the runtime should
        \\reclaim what a task holds rather than trusting it to, and until it does,
        \\something has to notice a task that was cancelled and never gave
        \\anything back.
        \\
    , .{});
}
