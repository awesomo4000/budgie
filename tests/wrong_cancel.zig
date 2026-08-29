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

/// 2. Ask with the wake reason instead of the state. NO LONGER EXPRESSIBLE.
///
/// This was the sharpest scene in the file. A task that checked
/// `reasonFor(t) == .cancelled` instead of `isCancelled(t)` did not miss the
/// cancellation occasionally, it missed it always: `makeRunnable` returns
/// early when the task is already queued, which is where a task making
/// progress spends its life, so the `.cancelled` wake was dropped and the
/// earlier `.spawn` stood. And it was a trap rather than an obvious error
/// because it worked perfectly for a task that was PARKED when the cancel
/// arrived. Correct for idle connections, silently wrong for busy ones.
///
/// `Wake` no longer has a `.cancelled` variant, so the check does not compile.
/// Writing this scene down is what made it obvious that it should not: the
/// same defect was quietly losing DEADLINES too, in code both servers ran, and
/// `expire` had already unlinked the timer by then, so the deadline vanished
/// with nothing left to fire it again.
fn asksWakeReason() void {
    scene("2. The task asks with the wake reason instead of the state");
    std.debug.print("  --    not expressible: Wake has no .cancelled variant to ask about\n", .{});
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

    // A token nobody filled in. Both servers set `live` by hand for their
    // listener without admitting it, so it sits at generation 0, and a
    // default-constructed token would otherwise name exactly that and cancel
    // the thing that accepts connections.
    const stale2 = s.cancels_stale;
    claim(!s.cancel(.none), "a token that names nothing cancels nothing", .{});
    claim(s.cancels_stale == stale2 + 1, "and is counted rather than ignored", .{});
}

pub fn main() !void {
    neverAsks();
    asksWakeReason();
    tokenBeforeAdmit();
    unwindOnTheWrongBudget();
    notMistakes();

    std.debug.print(
        \\
        \\Two ways left to get this wrong, and they are not two different bugs.
        \\
        \\  1 is the task not acting on it, and it is not expressible under
        \\    examples/cancel_supervised.zig, because there the task does not
        \\    ask and so cannot forget to.
        \\  2 is gone. It was asking with the wake reason, and Wake no longer
        \\    has a variant to ask about. Cancellation is state only.
        \\  3 is gone. It was a setup ordering error, and admission now returns
        \\    the only token there is, so holding one for a task that was never
        \\    admitted has nowhere to come from.
        \\  4 survives the dispatcher. An unwind that does not unwind is the
        \\    bug that gets relocated rather than removed.
        \\
        \\So: one bug removed by a dispatcher, one removed by deleting the way to
        \\express it, one ordering error removed by a return type, and one that
        \\no mechanism can remove, because no language can make a function do
        \\its job.
        \\
        \\On the cost, which has changed since this was written and is the sort
        \\of claim a catalogue goes stale by keeping. What these scenes measure
        \\is the bare scheduler API, where a task that never releases holds
        \\what it holds until the process ends. That is still true here and it
        \\is no longer true of the servers: `iobuf.Pool` records which task it
        \\handed each slot to and takes them back on teardown whatever the task
        \\did, reporting `bufs_stranded` when it has to. There, the same
        \\mistakes cost a missed courtesy rather than a leak.
        \\
        \\None of them, in either place, produces a wrong answer or makes
        \\anybody else wait.
        \\
    , .{});

    std.debug.print("{d} claims, {d} failures\n", .{ claims, failed });
    if (failed != 0) std.process.exit(1);
}
