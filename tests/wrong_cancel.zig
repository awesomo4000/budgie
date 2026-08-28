//! Every way I can find to get cancellation wrong here, and what each one
//! costs. `zig build wrong-cancel`.
//!
//! The point is the count. The ziggit thread that started this has at least
//! eight distinct ways to lose an `error.Canceled`: `try`, `catch {}`, the
//! flattening into `ReadFailed`, the wrong one of three error fields, the
//! wrong layer of a chain, a null slot that panics on unwrap, a chain whose
//! shape is decided at runtime, and a read whose failure is recorded in the
//! writer. Losing it costs you a task that never stops, with no diagnostic.
//!
//! If the answer here is also eight, the architecture has bought nothing and
//! this file is where that becomes obvious. So each scene below asserts what
//! the mistake actually costs, and the assertions fail if the cost changes.
//! A catalogue nobody runs is a catalogue that goes stale.
//!
//! Two of these stop being expressible under the dispatcher in
//! `examples/cancel_supervised.zig`, and a third stopped being expressible when
//! `admit` began returning the cancel token. The summary at the end says which.
//!
//! Scenes that become unwritable are kept as headings rather than deleted. The
//! shrinking list is the measurement.

const std = @import("std");
const budgie = @import("budgie");

const sched = budgie.sched;
const quota = budgie.quota;
const TaskId = sched.TaskId;

var s: sched.Sched = .{};
var q: quota.Tree = .{};

const t: TaskId = 1;
const cap: i64 = 1000;
const reserve: i64 = 50;
const quantum: i64 = 100;
const unwind_cost: i64 = 10;

var claims: usize = 0;
var failed: usize = 0;

fn scene(comptime title: []const u8) void {
    std.debug.print("\n{s}\n", .{title});
}

fn claim(ok: bool, comptime what: []const u8, detail: anytype) void {
    claims += 1;
    if (ok) {
        std.debug.print("  ok    {s}\n", .{what});
    } else {
        failed += 1;
        std.debug.print("  FAIL  {s}  {any}\n", .{ what, detail });
    }
}

/// What the task under test is holding and how far it got.
const W = struct {
    spent: i64 = 0,
    holds: bool = true,
    unwound: bool = false,
    unwind_charge: ?bool = null,
};
var w: W = .{};

fn reset() void {
    s = .{};
    q = .{};
    q.define(0, quota.none, quota.unlimited, .periodic, 100, "root");
    w = .{};
}

fn admitOne() sched.CancelTok {
    return s.admit(t, .{ .prio = 1, .quota = 0, .cap = cap, .reserve = reserve });
}

/// Real dispatch, not direct calls, because scene 2 turns on how the runnable
/// ring behaves and calling the step function by hand would hide it.
fn dispatch(comptime f: fn (TaskId) void, turns: usize) void {
    var n: usize = 0;
    while (n < turns) : (n += 1) {
        const id = s.popRunnable() orelse return;
        f(id);
    }
}

// ---------------------------------------------------------------- the unwinds

fn unwindProperly(id: TaskId) void {
    s.chargeReserve(id, unwind_cost);
    w.holds = false;
    w.unwound = true;
    s.release(id);
}

/// Pays for cleanup out of the body budget instead of the reserve.
fn unwindWithCharge(id: TaskId) void {
    w.unwind_charge = s.charge(id, &q, unwind_cost);
    if (!w.unwind_charge.?) return; // gives up partway, still holding
    w.holds = false;
    w.unwound = true;
    s.release(id);
}

// ------------------------------------------------------------------ the steps

fn stepNeverAsks(id: TaskId) void {
    if (!s.charge(id, &q, quantum)) return;
    w.spent += quantum;
    s.makeRunnable(id, .spawn);
}

fn stepAsksWakeReason(id: TaskId) void {
    if (s.reasonFor(id) == .cancelled) return unwindProperly(id);
    if (!s.charge(id, &q, quantum)) return;
    w.spent += quantum;
    s.makeRunnable(id, .spawn);
}

fn stepAsksState(id: TaskId) void {
    if (s.isCancelled(id)) return unwindProperly(id);
    if (!s.charge(id, &q, quantum)) return;
    w.spent += quantum;
    s.makeRunnable(id, .spawn);
}

fn stepUnwindsWithCharge(id: TaskId) void {
    if (s.isCancelled(id)) return unwindWithCharge(id);
    if (!s.charge(id, &q, quantum)) return;
    w.spent += quantum;
    s.makeRunnable(id, .spawn);
}

// ----------------------------------------------------------------- the scenes

/// 1. Never ask.
fn neverAsks() void {
    scene("1. The task never asks whether it was cancelled");
    reset();
    const tok = admitOne();
    dispatch(stepNeverAsks, 3);
    const before = w.spent;

    _ = s.cancel(tok);
    dispatch(stepNeverAsks, 500);

    claim(w.spent == before, "it does no further work: the units are gone, so it cannot", .{
        .before = before,
        .after = w.spent,
    });
    claim(w.holds and !w.unwound, "it never unwinds and never gives its resource back", .{
        .holds = w.holds,
    });
    claim(s.live[t] and s.cancelled[t], "it is still here, cancelled, indefinitely", .{});
}

/// 2. Ask with the wake reason instead of the state.
///
/// This is not an occasional miss. For any task that keeps itself runnable
/// across quanta, which is the ordinary pattern here and what every task in
/// both servers does, `reasonFor` NEVER reports `.cancelled`. `makeRunnable`
/// returns early when the task is already queued, so the `.cancelled` wake
/// that `cancel` tries to record is dropped on the floor and the earlier
/// `.spawn` stands.
///
/// One delivery, into a channel allowed to drop it. Exactly the shape the
/// whole design is supposed to avoid, reproduced inside this API by choosing
/// the wrong one of two queries.
fn asksWakeReason() void {
    scene("2. The task asks with the wake reason instead of the state");
    reset();
    const tok = admitOne();
    dispatch(stepAsksWakeReason, 3);

    // Cancel while the task is queued, which is where it spends most of its
    // life if it is making progress.
    claim(s.queued[t], "the task is queued, having made itself runnable again", .{});
    _ = s.cancel(tok);

    claim(s.isCancelled(t), "the state says cancelled", .{});
    claim(s.reasonFor(t) != .cancelled, "and the wake reason does not, and never will", .{
        .reason = s.reasonFor(t),
    });

    dispatch(stepAsksWakeReason, 500);
    claim(!w.unwound and w.holds, "so the task misses it completely and holds its resource", .{
        .unwound = w.unwound,
    });

    // And the reason it is a trap rather than an obvious mistake: it works
    // for a task that was PARKED when the cancel arrived. Not queued, so
    // `makeRunnable` records the reason, so `reasonFor` reports it. The check
    // is correct for idle connections and silently wrong for busy ones, which
    // is the worst way for a thing to be wrong.
    reset();
    const tok_parked = admitOne();
    _ = s.popRunnable(); // dequeue and park it, without making it runnable again
    s.arm(t, 1_000_000);
    claim(!s.queued[t], "a parked task is not queued", .{});
    _ = s.cancel(tok_parked);
    claim(s.reasonFor(t) == .cancelled, "so for a PARKED task the wake reason does report it", .{
        .reason = s.reasonFor(t),
    });
    dispatch(stepAsksWakeReason, 10);
    claim(w.unwound, "and the same broken check works here, which is why it gets trusted", .{});

    // The same task, one word different.
    reset();
    const tok2 = admitOne();
    dispatch(stepAsksState, 3);
    _ = s.cancel(tok2);
    dispatch(stepAsksState, 500);
    claim(w.unwound and !w.holds, "asking with isCancelled instead: unwound, resource returned", .{
        .unwound = w.unwound,
    });
}

/// 3. Mint the token before admission. NO LONGER EXPRESSIBLE.
///
/// This scene used to admit the task and then hand out a token minted against
/// the generation admission was about to bump, so the cancel silently did
/// nothing. It cannot be written any more: `admit` returns the only token
/// there is, and `cancelTok(id)` has been deleted. You cannot hold the right
/// to cancel a task that was never admitted, because there is nowhere to get
/// it from.
///
/// What survives is the case the generation counter is actually for, which is
/// a token outliving the task it names. That is in "Not mistakes" below, where
/// it belongs, because it is handled rather than merely counted.
fn tokenBeforeAdmit() void {
    scene("3. The token is minted before admission");
    std.debug.print("  --    not expressible: admit returns the only token there is\n", .{});
}

/// 4. Pay for cleanup out of the body budget.
fn unwindOnTheWrongBudget() void {
    scene("4. The unwind spends the body budget instead of the reserve");
    reset();
    const tok = admitOne();
    dispatch(stepUnwindsWithCharge, 3);
    _ = s.cancel(tok);
    dispatch(stepUnwindsWithCharge, 500);

    claim(w.unwind_charge != null and !w.unwind_charge.?, "the charge is refused: cancel zeroed the budget", .{
        .charge = w.unwind_charge,
    });
    claim(!w.unwound and w.holds, "so the unwind stops partway and the resource leaks", .{});
    claim(s.reserve[t] == reserve, "the reserve that would have paid for it is untouched", .{
        .reserve = s.reserve[t],
    });
}

/// Two things that look like mistakes and are not, which is also part of how
/// much there is to remember.
fn notMistakes() void {
    scene("Not mistakes");
    reset();
    const tok = admitOne();

    claim(s.cancel(tok), "first cancel takes", .{});
    claim(s.cancel(tok), "second cancel is idempotent and reports success", .{});
    claim(s.cancels == 1, "and does not count twice or wake twice", .{ .cancels = s.cancels });

    dispatch(stepAsksState, 10);
    claim(w.unwound, "the task unwound and released its slot", .{});

    // The slot is free and something else takes it.
    _ = admitOne();
    const stale_before = s.cancels_stale;
    claim(!s.cancel(tok), "the old token does not cancel whoever inherited the slot", .{});
    claim(s.cancels_stale == stale_before + 1, "it is counted as stale", .{});
    claim(!s.isCancelled(t), "and the new occupant is untouched", .{});
}

pub fn main() !void {
    neverAsks();
    asksWakeReason();
    tokenBeforeAdmit();
    unwindOnTheWrongBudget();
    notMistakes();

    std.debug.print(
        \\
        \\Three ways left to get this wrong, and they are not three different bugs.
        \\
        \\  1 and 2 are the same bug: the task did not act on it. Neither is
        \\    expressible under examples/cancel_supervised.zig, because there
        \\    the task does not ask and so cannot forget to, or ask wrongly.
        \\  3 is gone. It was a setup ordering error, and admission now returns
        \\    the only token there is, so holding one for a task that was never
        \\    admitted has nowhere to come from.
        \\  4 survives the dispatcher. An unwind that does not unwind is the
        \\    bug that gets relocated rather than removed.
        \\
        \\So: one bug, removed by a dispatcher. One ordering error, removed by a
        \\return type. And one that no mechanism can remove, because no language
        \\can make a function do its job. The cost of all of them is the same and it is
        \\bounded: a task that holds a resource forever. None of them produces
        \\a wrong answer, and none makes anybody else wait.
        \\
    , .{});

    std.debug.print("{d} claims, {d} failures\n", .{ claims, failed });
    if (failed != 0) std.process.exit(1);
}
