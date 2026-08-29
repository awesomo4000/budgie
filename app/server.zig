//! The application, and the kernel step that ties scheduler to reactor.
//!
//! The step is now four lines and mentions neither fds nor phases:
//!
//!     while (sched.popRunnable()) |t| app.run(t);
//!     const timeout = sched.timeoutMs(now);
//!     reactor.wait(&sched, timeout);
//!     sched.expire(now);
//!
//! Everything socket-shaped lives below `run`. Everything runnability-shaped
//! lives in sched.zig. Swapping poll for epoll touches reactor.zig only.

const std = @import("std");
const linux = std.os.linux;
const Sched = @import("budgie").sched.Sched;
const TaskId = @import("budgie").sched.TaskId;
const max_tasks = @import("budgie").sched.max_tasks;
const Reactor = @import("budgie").reactor.Reactor;
const quota = @import("budgie").quota;
const acct = @import("budgie").accounts;
const Label = acct.Label;
const prio_idle = @import("budgie").sched.prio_idle;
const sched = @import("budgie").sched;

// Supervisor tree. root bounds the whole process; the classes below it can be
// tuned against each other without any of them being able to exceed the root.
const sup_root: quota.Id = 0;
const sup_conn: quota.Id = 1;   // ALL connections together
const sup_bg: quota.Id = 2;
const sup_ctrl: quota.Id = 3;
const posix = std.posix;
const http = @import("budgie").http;
const Clock = @import("budgie").clock.Clock;
const iobuf = @import("budgie").iobuf;
const sys = @import("budgie").sys;

/// Every knob is runtime-settable so a sweep needs no recompile. Defaults
/// match the values the earlier runs used.
const Knobs = struct {
    work_budget: i64 = 1000,
    cleanup_reserve: i64 = 50,
    quantum_units: i64 = 250,
    idle_deadline_ms: i64 = 3000,
    bg_budget: i64 = 400,
    bg_period_ms: i64 = 50,
    bg_quantum: i64 = 100,
    tick_ms: u64 = 20,
    accept_burst: usize = 512,
    /// Bytes a connection may receive per DRR round. 0 = no fairness
    /// enforcement (the original behaviour).
    /// DRR byte fairness. 0 = off, and OFF IS THE DEFAULT ON THIS BACKEND.
    ///
    /// SIX attempts, six distinct failures. The most recent and most
    /// diagnostic: with the tick-driven token bucket the greedy client gets
    /// EXACTLY 6 requests at every quantum from 512 to 4096. Quantum-
    /// independent means it is not a tuning problem -- a throttled connection
    /// is not being resumed. `advanceRound` reports 700 rounds and returns
    /// eligible flows, and `drrResume` re-arms the read interest, so the
    /// resume path looks correct and demonstrably is not. That is where the
    /// next attempt should start: instrument the resume, do not tune.
    ///
    /// The policy in drr.zig is backend-neutral and the enforcement here (do
    /// not re-arm the read interest) is correct and cheap. What does NOT
    /// transfer is the tuning. Measured, polite(depth 1) vs greedy(depth 256),
    /// polite's share of a fair 50%:
    ///
    ///     quantum   16384   24576   32768   49152   65536
    ///     share       97%     93%   65-92%  31-52%    34%
    ///
    /// A narrow, unstable window with no setting that reliably lands near 50%,
    /// while the SAME policy and quantum give 58-67% on the completion
    /// backend. Capping the read to match completion granularity (2 KB) did
    /// not unify them either -- it moved epoll to 8%.
    ///
    /// Root cause: DRR's effective rate is `quantum x round_rate`, and
    /// round_rate is an emergent property of each backend's wake pattern
    /// rather than a controlled parameter. Textbook DRR advances a round after
    /// visiting every backlogged flow ONCE; ours advances whenever the loop is
    /// about to block, which is a different thing per backend. Fixing that --
    /// counting distinct flows served since the last round and advancing at
    /// n_active -- is the change that would make one quantum mean the same
    /// thing everywhere. Not done.
    drr_quantum: i64 = 0,
    /// 0 = advance on block (works), 1 = advance on service (does NOT --
    /// see drr.zig RoundPolicy). Default 0.
    drr_policy: i64 = 0,
    /// 1 = end a round when every backlogged client has been read once
    /// (epoll supplies the backlogged set). 0 = credit on the tick.
    service_rounds: i64 = 0,

    /// 0 = the background task honours the yield contract.
    /// N > 0 = it runs N times longer and never yields, so the watchdog has
    /// something real to catch.
    bg_rude: i64 = 0,
    /// 1 = disarm the itimer while blocked, 0 = leave it running always.
    lazy_tick: i64 = 1,
    /// Aggregate cap for the whole connection class, units per sup_period_ms.
    /// 0 = unmetered (the old per-task-only behaviour).
    conn_quota: i64 = 0,
    sup_period_ms: i64 = 100,
    grant: i64 = 1000,
    /// How many connections may have I/O in flight at once. This IS the
    /// memory budget for buffers.
    /// Buffer pool size.
    ///
    /// The READINESS build needs roughly one buffer PER CONNECTION, because
    /// `stepReading` acquires before reading, so every readable connection
    /// holds one at the same instant. Measured at 256 connections: io_bufs=64
    /// recorded 95,522 `no_buffer` endings at 16k req/s; io_bufs=256 recorded
    /// zero at 87k.
    ///
    /// Those were counted endings, and at the time an ending was not the same
    /// thing as an answer. `.no_buffer` was missing from the switch that picks
    /// the status line, so it fell through to the deadline arm, and the unwind
    /// had no buffer to format into anyway, so most of those connections were
    /// closed in silence. Both are fixed and a socket test holds them fixed,
    /// but the 95,522 figure is a count of refusals the server decided on, not
    /// of 503s a client received.
    ///
    /// The COMPLETION build is the opposite -- it acquires in `onData`, only
    /// once bytes have actually arrived, and processes completions serially,
    /// so its high-water mark was 64 against 256 here. It runs best with a
    /// SMALL pool (see APIGUIDE: fewer buffers is also a latency win there).
    /// Same knob, opposite optimum, for a structural reason.
    io_bufs: i64 = 1024,
    /// 1 = append pipelined answers into one buffer and issue a single
    /// `write` for the run, instead of one `write` per answer.
    ///
    /// The phase accounting says the write syscall is essentially the whole
    /// CPU cost of this server: 5.39s of a 12s run at ~3.5us per call, with
    /// parse at 0.32s and everything else rounding to nothing. A pipelined
    /// client hands over a queue of requests whose answers all go to the same
    /// descriptor, so they can share one call.
    out_batch: i64 = 1,
    /// Kernel send buffer for an accepted connection, in bytes. 0 leaves the
    /// system default, which on both platforms auto-tunes into the megabytes.
    ///
    /// It is here because backpressure is otherwise unreachable. A test that
    /// wants the server to meet EAGAIN has to leave several megabytes of
    /// response unread, and the buffer keeps growing while it tries. Setting
    /// this bounds the per-connection kernel memory, which is the same kind of
    /// decision `io_bufs` is, and makes the parked-write path something a test
    /// can provoke in one round trip.
    send_buf_bytes: i64 = 0,
    /// 1 = break the drain loop when the tick fires, so the reactor is
    /// re-entered and a waiting control task is seen within one tick.
    /// 0 = drain to completion, control latency then bounded only by how much
    /// lower-priority work happened to be runnable.
    bounded_drain: i64 = 1,
};
var K: Knobs = .{};
const listener_task: TaskId = 0;
const background_task: TaskId = 1;
const ctrl_listener_task: TaskId = 2;

/// Priority classes. The control surface sits above everything, including
/// accept: a keypress must not queue behind a connection burst.
const prio_ctrl: u8 = 0;
const prio_listen: u8 = 1;
const prio_conn: u8 = 2;

/// The background task is rate-limited exactly the way an seL4 scheduling
/// context is: a budget that refills on a period. It cannot starve connections
/// no matter how much work it wants to do.


const sockaddr_in = sys.SockAddrIn;
const sysErr = sys.sysErr;

// ---------------------------------------------------------------- the tick
//
// The only asynchrony in the program. The handler touches four atomics and
// clock_gettime; nothing else here has to be async-signal-safe, because
// `in_kernel` plus a pending counter defers all real work to the next kernel
// entry.

const Shared = struct {
    in_kernel: std.atomic.Value(bool) = .init(true),
    pending: std.atomic.Value(u32) = .init(0),
    arrived_ns: std.atomic.Value(i64) = .init(0),
    took_in_kernel: std.atomic.Value(u64) = .init(0),
    took_in_task: std.atomic.Value(u64) = .init(0),

    // ---------------------------------------------------------- yield watchdog
    //
    // The property we care about is NOT "did a task switch happen" -- a
    // long-running task that yields diligently but is never preempted, because
    // nothing higher priority wanted to run, produces zero switches and is
    // behaving perfectly. The property is: did the running task REACH A YIELD
    // POINT within the last tick?
    //
    // So the yield is instrumented, not the dispatch. `yield_seq` is monotonic
    // and only ever incremented by the mutator; the handler only READS it and
    // compares against its own private snapshot, so there is no
    // read-modify-write in signal context and no race.
    //
    // `should_yield` is the flag the yield fast path was already loading, so
    // the common case costs nothing new: one load of a usually-zero word and a
    // branch that is usually not taken.
    should_yield: std.atomic.Value(u32) = .init(0),
    yield_seq: std.atomic.Value(u64) = .init(0),

    /// (dispatch_seq << 20) | task_id, stored by the kernel at each dispatch.
    /// One load in the handler, cannot tear, and it names the culprit rather
    /// than just reporting that someone broke the contract.
    running: std.atomic.Value(u64) = .init(0),

    // Handler-private state. Only `onTick` touches these.
    last_yield_seq: u64 = 0,
    last_running: u64 = 0,
    missed_ticks: u32 = 0,

    // Reported to the app.
    stall_warnings: std.atomic.Value(u64) = .init(0),
    stall_faults: std.atomic.Value(u64) = .init(0),
    worst_missed: std.atomic.Value(u32) = .init(0),
    culprit: std.atomic.Value(u64) = .init(0),
    yields_total: std.atomic.Value(u64) = .init(0),
};

pub const stall_fault_after: u32 = 3; // consecutive missed ticks -> fault
var shared: Shared = .{};

fn onTick(_: posix.SIG) callconv(.c) void {
    const in_k = shared.in_kernel.load(.monotonic);
    if (in_k) {
        _ = shared.took_in_kernel.fetchAdd(1, .monotonic);
    } else {
        _ = shared.took_in_task.fetchAdd(1, .monotonic);
    }

    // Ask the running task to yield at its next opportunity.
    shared.should_yield.store(1, .release);

    // Watchdog. Only meaningful while a task is running: a thread parked in
    // the reactor legitimately passes no yield points, and `lazy_tick`
    // disarms the timer before a real sleep so this should not fire then --
    // but check `in_kernel` anyway rather than depend on that.
    if (!in_k) {
        const seq = shared.yield_seq.load(.monotonic);
        const run = shared.running.load(.monotonic);
        if (seq == shared.last_yield_seq and run == shared.last_running) {
            shared.missed_ticks += 1;
            if (shared.missed_ticks > shared.worst_missed.load(.monotonic))
                shared.worst_missed.store(shared.missed_ticks, .monotonic);
            shared.culprit.store(run, .monotonic);
            if (shared.missed_ticks == 1) {
                _ = shared.stall_warnings.fetchAdd(1, .monotonic);
            } else if (shared.missed_ticks == stall_fault_after) {
                // Not a kill. A fault the supervisor decides about, in the
                // seL4 timeout-fault shape: the task is not destroyed and its
                // state is intact.
                _ = shared.stall_faults.fetchAdd(1, .monotonic);
            }
        } else {
            shared.missed_ticks = 0;
        }
        shared.last_yield_seq = seq;
        shared.last_running = run;
    } else {
        shared.missed_ticks = 0;
    }
    if (shared.pending.fetchAdd(1, .acq_rel) == 0) {
        shared.arrived_ns.store(nowNs(), .monotonic);
    }
}

const armTimer = sys.armIntervalTimer;

const rssKb = sys.rssKb;

const cpuMs = sys.cpuMs;

/// Kept as free functions so the diff stays small, but they now read the
/// process clock rather than the system clock. `app_storage.clock` is the only
/// time source in the program.
fn nowMs() i64 {
    return app_storage.clock.ms();
}

fn nowNs() i64 {
    return app_storage.clock.ns();
}

fn realNowMs() i64 {
    return @divTrunc(@import("budgie").clock.monotonicNs(), 1_000_000);
}

/// The yield primitive.
///
/// Fast path: one load of a usually-zero word and a branch that is usually not
/// taken -- the same load the caller would have done anyway to ask "should I
/// stop?". Nothing is incremented on the fast path.
///
/// Slow path (the tick asked): bump the monotonic yield sequence so the
/// watchdog can see that a yield point was reached, clear the request, and
/// tell the caller to give the turn back.
///
/// Returns true if the caller should stop and return to the scheduler.
pub inline fn yieldCheck() bool {
    if (shared.should_yield.load(.acquire) == 0) return false;
    shared.should_yield.store(0, .release);
    _ = shared.yield_seq.fetchAdd(1, .monotonic);
    _ = shared.yields_total.fetchAdd(1, .monotonic);
    return true;
}

/// Called by a task that wants to record progress even when the tick has not
/// asked -- useful at coarse boundaries so the watchdog sees liveness.
pub inline fn yieldMark() void {
    _ = shared.yield_seq.fetchAdd(1, .monotonic);
}

const Phase = enum { reading, working, writing, cleanup };
const Ending = enum { ok, budget_exhausted, deadline_missed, cancelled, bad_request, no_buffer, peer_gone };

const Conn = struct {
    fd: i32,
    is_ctrl: bool = false,
    phase: Phase = .reading,
    ending: Ending = .ok,
    work_left: i64 = 0,
    spent: i64 = 0,
    sink: u64 = 0,
    /// Null while parked with nothing in flight. Acquired on the first read
    /// of a request, released when the connection goes idle again.
    buf: iobuf.Handle = .{},
    /// The right to cancel this connection, handed back by admission. The
    /// server holds it because the server admitted the task; nothing else in
    /// the program can mint one. `.none` until accept fills it in, and a
    /// `.none` token cancels nothing.
    tok: sched.CancelTok = .none,
};


const App = struct {
    s: Sched = .{},
    r: Reactor = .{},
    live_conn: [max_tasks]bool = @splat(false),
    conn_store: *[max_tasks]Conn = undefined,
    listener: i32,
    free_hint: TaskId = 3,
    quiet: bool = true,

    steps: u64 = 0,
    accepted: u64 = 0,
    bg_iters: u64 = 0,
    bg_sink: u64 = 0,
    bg_starved: u64 = 0,
    tick_armed: bool = false,
    tick_arms: u64 = 0,
    tick_disarms: u64 = 0,
    /// True when the previous block was shorter than a tick period. Disarming
    /// costs two syscalls; leaving the timer armed costs one wakeup per tick.
    /// If we keep waking quickly, the disarm/rearm pair is the more expensive
    /// of the two, so skip it. Keying off the REQUESTED timeout does not work:
    /// under load it is still 1000ms because connection deadlines are seconds
    /// away, yet the actual block is microseconds.
    blocks_short: bool = false,
    slept_ms: i64 = 0,
    clock: Clock = Clock.real(),
    bufs: iobuf.Pool = .{},
    q: quota.Tree = .{},
    book: acct.Book = .{},
    ctrl_listener: i32 = -1,
    drain_breaks: u64 = 0,
    dispatch_seq: u64 = 0,
    endings: [7]u64 = @splat(0),
    ticks_drained: u64 = 0,
    tick_coalesced: u64 = 0,
    defer_max_ns: i64 = 0,
    served: u64 = 0,
    batched_answers: u64 = 0,
    /// Backpressure, in its two shapes, counted so a test can assert the path
    /// ran rather than assuming a small buffer was enough to provoke it. A
    /// write that moved some bytes and stopped leaves `out_sent` partway
    /// through and has to resume; a write that moved none found the peer's
    /// window shut. They are separate counters because they are separate
    /// paths, and a test that only reaches one of them has only covered one.
    partial_writes: u64 = 0,
    write_stalls: u64 = 0,
    /// Buffers the pool had to take back because the connection ended still
    /// holding them. Should be zero; a number here names an unwind that does
    /// not do its job.
    bufs_stranded: usize = 0,

    // ----------------------------------------------------------- the kernel

    fn step(a: *App) void {
        a.steps += 1;

        // 0. Drain the tick. Read the timestamp before clearing the counter so
        //    a signal landing in the window over-reports latency rather than
        //    under-reporting it.
        const arrived = shared.arrived_ns.load(.monotonic);
        const drained = shared.pending.swap(0, .acquire);
        if (drained != 0) {
            const lat = nowNs() - arrived;
            if (lat > a.defer_max_ns) a.defer_max_ns = lat;
            a.ticks_drained += 1;
            a.tick_coalesced += drained;
            // Credit on the TICK, not on "about to block".
            //
            // Advancing when the loop is about to block makes the round rate a
            // function of how much work is pending, which is a function of the
            // quantum -- so the knob cancels itself. Measured: quantum 1024 ->
            // 16384 moved rounds 1088 -> 305 and the fairness share not at
            // all. The tick is a rate the quantum cannot influence, so the
            // per-client rate is quantum/tick and is actually controllable.
            if (a.r.quantum > 0 and !a.r.service_rounds) _ = a.r.advanceRound();
        }

        // 1. Drain by priority. Higher classes drain completely; the idle
        //    class gets at most ONE quantum per step, then we go back to the
        //    reactor. Without that break an idle task that re-queues itself
        //    would spin the drain loop forever and epoll_wait would never be
        //    reached -- strict priority with a self-renewing bottom task is a
        //    livelock unless the loop is bounded somewhere.
        shared.in_kernel.store(false, .release);
        while (a.s.popRunnable()) |t| {
            const was_idle = a.s.prio[t] == prio_idle;
            a.run(t);
            if (was_idle) break;
            // The tick is the deadline for re-entering the reactor. Without
            // this the drain runs to completion and a keypress waits behind
            // every runnable connection.
            if (K.bounded_drain != 0 and shared.pending.load(.acquire) != 0) {
                a.drain_breaks += 1;
                break;
            }
        }
        shared.in_kernel.store(true, .release);

        // Service-counted round: every backlogged client has had a turn.
        if (a.r.roundComplete()) _ = a.r.advanceRound();

        // 2. How long may we sleep? O(1) peek at the deadline heap.
        const now = nowMs();
        a.q.refillPeriodic(now);
        const timeout: i32 = if (a.s.anyRunnable())
            0
        else if (a.s.timeoutMs(now)) |ms|
            @intCast(@min(1000, ms))
        else
            1000;

        // 3. Block. Before a real sleep, disarm the tick: it exists to bound
        //    compute, and a thread parked in epoll_wait is running none. An
        //    always-on 20ms itimer is 50 wakeups/s that an idle server pays
        //    for nothing.
        if (timeout > 0 and a.tick_armed and K.lazy_tick != 0 and !a.blocks_short) {
            armTimer(0);
            a.tick_armed = false;
            a.tick_disarms += 1;
        }
        const t_sleep = nowMs();
        _ = a.r.wait(&a.s, timeout);
        if (timeout > 0) {
            const slept = nowMs() - t_sleep;
            a.slept_ms += slept;
            a.blocks_short = slept < @as(i64, @intCast(K.tick_ms));
        }
        if (a.s.anyRunnable() and !a.tick_armed and K.lazy_tick != 0) {
            armTimer(K.tick_ms);
            a.tick_armed = true;
            a.tick_arms += 1;
        }

        // 4. Expire deadlines. O(expired * log n).
        a.s.expire(nowMs());
    }

    fn run(a: *App, t: TaskId) void {
        a.dispatch_seq +%= 1;
        shared.running.store((@as(u64, a.dispatch_seq) << 20) | @as(u64, t), .release);
        if (t == listener_task) return a.doAccept();
        if (t == background_task) return a.stepBackground(t);
        if (t == ctrl_listener_task) return a.acceptCtrl();
        if (!a.live_conn[t]) return;
        const c = &a.conn_store[t];

        // THE FIX. Previously this checked only `.deadline`, and only that
        // condition. A cancelled task in `.writing` would never notice:
        // stepWriting calls no `charge`, so a zeroed budget is invisible to
        // it, and it would keep writing forever.
        //
        // Both terminating conditions are now checked unconditionally, before
        // the phase switch, in runtime code the app never edits. Per-phase
        // checking is the same shape of mistake as per-call-site `catch`.
        if (c.phase != .cleanup) {
            if (a.s.isCancelled(t)) return a.enterCleanup(t, c, .cancelled);
            if (a.s.isExpired(t)) return a.enterCleanup(t, c, .deadline_missed);
        }
        if (c.is_ctrl) return a.stepCtrl(t, c);
        switch (c.phase) {
            .reading => a.stepReading(t, c),
            .working => a.stepWorking(t, c),
            .writing, .cleanup => a.stepWriting(t, c),
        }
    }

    // ------------------------------------------------------------- listener

    fn doAccept(a: *App) void {
        const t0 = nowNs();
        defer a.book.observe(.accept, nowNs() - t0);
        var burst: usize = 0;
        while (burst < K.accept_burst) : (burst += 1) {
            const rc = sys.acceptNonblock(a.listener);
            if (sysErr(rc)) break;
            const fd: i32 = @intCast(rc);
            if (K.send_buf_bytes > 0) sys.setSendBuf(fd, @intCast(K.send_buf_bytes));
            const t = a.freeTask() orelse {
                sys.close(fd);
                break;
            };
            a.conn_store[t] = .{ .fd = fd };
            a.live_conn[t] = true;
            a.conn_store[t].tok = a.s.admit(t, .{
                .prio = prio_conn,
                .quota = sup_conn,
                .cap = K.work_budget,
                .reserve = K.cleanup_reserve,
            });
            a.r.open(t);
            a.s.arm(t, nowMs() + K.idle_deadline_ms);
            a.accepted += 1;
        }
        a.r.watch(listener_task, a.listener, .read);
    }

    /// A CPU-bound task with no I/O at all. It exists to give the tick
    /// something to preempt, and to show that background work throttled by a
    /// refilling budget cannot starve the connections it shares a core with.
    fn stepBackground(a: *App, t: TaskId) void {
        // A refill deadline arrived: top the budget back up. Same wheel, same
        // mechanism as a connection's idle timeout -- only the meaning differs.
        //
        // This was `reasonFor(t) == .deadline`, and it was silently broken: a
        // refill that fired while this task was queued had its wake reason
        // dropped, so the top-up never happened and the re-arm below never
        // ran. One missed edge and background work stopped for the life of the
        // process. Reading the state instead cannot miss it.
        if (a.s.isExpired(t)) {
            _ = a.s.topUp(t, &a.q, K.bg_budget);
            a.s.arm(t, nowMs() + K.bg_period_ms);
        }

        const t0 = nowNs();
        if (!a.chargeAs(t, .background, K.bg_quantum)) {
            // Out of budget for this period. Park until the refill fires --
            // it does not spin, and it does not steal from connections.
            a.bg_starved += 1;
            a.book.observe(.background, nowNs() - t0);
            return;
        }
        // Sequentially dependent so it cannot be folded to a closed form.
        var j: i64 = 0;
        const limit = K.bg_quantum * 2000 * @max(1, K.bg_rude);
        while (j < limit) : (j += 1) {
            a.bg_sink = a.bg_sink *% 6364136223846793005 +% 1442695040888963407;
            // The cooperative contract: check often, cheaply. With bg_rude > 1
            // this check is skipped, which is exactly the violation the
            // watchdog exists to name.
            if (K.bg_rude == 0 and (j & 0xfff) == 0 and yieldCheck()) break;
        }
        a.bg_iters += 1;
        a.book.observe(.background, nowNs() - t0);
        a.s.makeRunnable(t, .spawn);
    }

    /// The control surface. Echoes whatever arrives, immediately, at the top
    /// priority class. No parsing, no budget, no work phase -- a control
    /// surface that can be made to do arbitrary work is not a control surface.
    fn acceptCtrl(a: *App) void {
        while (true) {
            const rc = sys.acceptNonblock(a.ctrl_listener);
            if (sysErr(rc)) break;
            const fd: i32 = @intCast(rc);
            const t = a.freeTask() orelse {
                sys.close(fd);
                break;
            };
            a.conn_store[t] = .{ .fd = fd, .is_ctrl = true };
            a.live_conn[t] = true;
            _ = a.s.admit(t, .{ .prio = prio_ctrl, .quota = sup_ctrl, .cap = 1 << 20, .reserve = 1 << 20 });
            a.r.watch(t, fd, .read);
        }
        a.r.watch(ctrl_listener_task, a.ctrl_listener, .read);
    }

    /// The outcome of a shed. `busy` is how many of the cancelled connections
    /// had I/O in flight at the moment they were cancelled, which is decided
    /// in the same pass as the cancel rather than sampled by the caller
    /// before or after. A test that wants to prove it cancelled work in
    /// progress cannot do that from outside without racing the server; this
    /// makes it a fact the server reports.
    const Shed = struct { cancelled: usize = 0, busy: usize = 0 };

    /// Shed load: withdraw the authority to run from up to `want` connections.
    ///
    /// This is the case a deadline cannot cover. Every connection here is
    /// healthy, responsive and inside its budget; somebody outside has simply
    /// decided there should be fewer of them. The scheduler zeroes their
    /// budgets, so they cannot serve another byte whether or not they notice,
    /// and each one unwinds on its reserve and answers 503 before closing.
    ///
    /// It cancels with tokens the server was handed at admission. There is no
    /// way to cancel a task this server did not admit, which is the point of
    /// admission returning the token rather than a `cancelTok(id)` anyone
    /// could call.
    fn shed(a: *App, want: usize) Shed {
        var out: Shed = .{};
        var t: TaskId = 0;
        while (t < max_tasks and out.cancelled < want) : (t += 1) {
            if (!a.live_conn[t]) continue;
            const c = &a.conn_store[t];
            // Not the control connection giving the order, and not one that
            // is already on its way out.
            if (c.is_ctrl or c.phase == .cleanup) continue;
            // Holding a buffer means bytes are in flight: the pool hands one
            // out on the first read of a request and takes it back when the
            // connection goes idle again.
            const was_busy = !c.buf.isNull();
            if (a.s.cancel(c.tok)) {
                out.cancelled += 1;
                if (was_busy) out.busy += 1;
            }
        }
        return out;
    }

    fn stepCtrl(a: *App, t: TaskId, c: *Conn) void {
        var buf: [64]u8 = undefined;
        const rc = sys.read(c.fd, &buf, buf.len);
        if (sysErr(rc)) {
            if (!sys.wouldBlock(rc)) {
                a.r.unwatch(t);
                a.s.release(t);
                sys.close(c.fd);
                a.live_conn[t] = false;
                return;
            }
            return a.r.watch(t, c.fd, .read);
        }
        if (rc == 0) {
            a.r.unwatch(t);
            a.s.release(t);
            sys.close(c.fd);
            a.live_conn[t] = false;
            return;
        }
        // One command, and everything else still echoes, which is what the
        // existing control-surface test checks.
        const line = buf[0..rc];
        if (std.mem.startsWith(u8, line, "shed ")) {
            var end: usize = 5;
            while (end < line.len and line[end] >= '0' and line[end] <= '9') end += 1;
            const want = std.fmt.parseInt(usize, line[5..end], 10) catch 0;
            var out: [64]u8 = undefined;
            const r = a.shed(want);
            const reply = std.fmt.bufPrint(&out, "shed {d} busy {d}\n", .{ r.cancelled, r.busy }) catch
                "shed 0 busy 0\n";
            _ = sys.write(c.fd, reply.ptr, reply.len);
        } else {
            _ = sys.write(c.fd, &buf, rc);
        }
        a.r.watch(t, c.fd, .read);
    }

    /// Charge execution units and label them. The scheduler does the first
    /// half and knows nothing about the label; the app does the second.
    fn chargeAs(a: *App, t: TaskId, l: Label, units: i64) bool {
        const ok = a.s.charge(t, &a.q, units);
        a.book.charged(l, units, ok);
        return ok;
    }

    fn freeTask(a: *App) ?TaskId {
        var i = a.free_hint;
        var n: usize = 0;
        while (n < max_tasks - 1) : (n += 1) {
            if (!a.live_conn[i]) {
                a.free_hint = if (i + 1 >= max_tasks) 3 else i + 1;
                return i;
            }
            i = if (i + 1 >= max_tasks) 3 else i + 1;
        }
        return null;
    }

    // --------------------------------------------------------------- phases

    fn park(a: *App, t: TaskId, c: *Conn, i: @import("budgie").reactor.Interest) void {
        a.r.watch(t, c.fd, i);
    }

    /// Release the buffer when a connection goes idle with nothing pending.
    /// This is the whole saving: a parked keep-alive connection holds none.
    fn releaseIfIdle(a: *App, c: *Conn) void {
        if (c.buf.isNull()) return;
        const b = a.bufs.get(c.buf) orelse return;
        if (b.in_len != 0 or b.out_sent != b.out_len) return;
        a.bufs.release(c.buf);
        c.buf = .{};
    }

    /// The driver: I/O and budget, nothing else. All protocol knowledge lives
    /// in http.Parser, which never sees this fd.
    ///
    /// The unconsumed tail must survive across requests. `feed` returning less
    /// than it was given is the normal pipelined case, and dropping the
    /// remainder silently loses the next request -- which is precisely what
    /// the first version of this function did.
    fn stepReading(a: *App, t: TaskId, c: *Conn) void {
        if (c.buf.isNull()) {
            // Buffer exhaustion is an admission decision with a real answer,
            // not an allocation failure. Same shape as budget exhaustion.
            c.buf = a.bufs.acquireFor(t) orelse return a.enterCleanup(t, c, .no_buffer);
        }
        const b = a.bufs.get(c.buf) orelse return a.finish(t, c, .peer_gone);

        if (b.in_len > 0 and a.drainParser(t, c, b)) return;

        if (b.in_len == b.in.len) return a.enterCleanup(t, c, .bad_request);
        const rc: usize = @bitCast(a.r.read(t, c.fd, b.in[b.in_len..]));
        if (sysErr(rc)) {
            if (!sys.wouldBlock(rc)) return a.finish(t, c, .peer_gone);
            a.releaseIfIdle(c);
            return a.park(t, c, .read);
        }
        if (rc == 0) return a.finish(t, c, .peer_gone);
        b.in_len += rc;

        if (a.drainParser(t, c, b)) return;
        a.park(t, c, .read);
    }

    /// Feeds buffered bytes to the parser, consuming exactly what it takes and
    /// keeping the rest. Returns true if the connection changed phase.
    fn drainParser(a: *App, t: TaskId, c: *Conn, b: *iobuf.IoBuf) bool {
        const t0 = nowNs();
        defer a.book.observe(.parse, nowNs() - t0);
        while (b.in_len > 0) {
            const used = b.parser.feed(b.in[0..b.in_len]);
            if (used > 0) {
                std.mem.copyForwards(u8, b.in[0 .. b.in_len - used], b.in[used..b.in_len]);
                b.in_len -= used;
                // Charged against bytes the machine says it consumed -- an
                // honest quantity, not a made-up constant.
                const units = @divTrunc(@as(i64, @intCast(used)), 64) + 1;
                if (!a.chargeAs(t, .parse, units)) {
                    a.enterCleanup(t, c, .budget_exhausted);
                    return true;
                }
            }
            switch (b.parser.poll()) {
                .need_input => if (used == 0) return false else continue,
                .protocol_error => {
                    a.enterCleanup(t, c, .bad_request);
                    return true;
                },
                .request => |req| {
                    c.work_left = req.work_units;
                    c.phase = .working;
                    a.s.makeRunnable(t, .spawn);
                    return true;
                },
            }
        }
        return false;
    }

    fn stepWorking(a: *App, t: TaskId, c: *Conn) void {
        const t0 = nowNs();
        defer a.book.observe(.work, nowNs() - t0);
        const charge = @min(K.quantum_units, c.work_left);
        if (charge > 0 and !a.chargeAs(t, .work, charge)) {
            return a.enterCleanup(t, c, .budget_exhausted);
        }
        c.spent += charge;
        c.work_left -= charge;
        var j: i64 = 0;
        while (j < charge * 200) : (j += 1)
            c.sink = c.sink *% 6364136223846793005 +% 1442695040888963407;

        if (c.work_left > 0) return a.s.makeRunnable(t, .spawn);

        const b = a.bufs.get(c.buf) orelse return a.finish(t, c, .peer_gone);

        // Append, rather than overwrite at zero. Everything already in `out`
        // is an answer to an earlier request on this same connection that has
        // not been written yet.
        const answer = std.fmt.bufPrint(b.out[b.out_len..],
            "HTTP/1.1 200 OK\r\nContent-Length: 24\r\n\r\ndone, spent {d:>5} units\n", .{c.spent}) catch
            return a.finish(t, c, .peer_gone);
        b.out_len += answer.len;
        a.served += 1;

        // Another request is already buffered and there is room for its
        // answer, so take it now and let both go out together. The per-request
        // state resets here exactly as it does after a write.
        if (K.out_batch != 0 and b.in_len > 0 and b.out.len - b.out_len >= max_answer_bytes) {
            b.parser.reset();
            c.spent = 0;
            c.work_left = 0;
            c.phase = .reading;
            a.s.setReserve(t, K.cleanup_reserve);
            a.s.renewCap(t, K.work_budget);
            a.s.arm(t, nowMs() + K.idle_deadline_ms);
            a.batched_answers += 1;
            return a.s.makeRunnable(t, .spawn);
        }

        b.out_sent = 0;
        c.phase = .writing;
        a.s.makeRunnable(t, .spawn);
    }

    /// Worst case for one answer, so the batching loop knows when to stop.
    const max_answer_bytes = 96;

    fn enterCleanup(a: *App, t: TaskId, c: *Conn, why: Ending) void {
        c.ending = why;
        const t0 = nowNs();
        defer a.book.observe(.cleanup, nowNs() - t0);
        c.phase = .cleanup;
        a.s.chargeReserve(t, 10);
        // A cancelled task parked in the interest set must come out of it.
        // That unwatch is itself a cleanup-reserve action.
        a.r.unwatch(t);
        a.s.arm(t, nowMs() + 500);
        // `.no_buffer` was falling through to the deadline arm, so a pool
        // that ran dry answered "408 Request Timeout" to a client whose
        // request had not timed out. The pool is an admission control, and
        // the honest answer for admission refused is 503.
        const body = switch (why) {
            .bad_request => "400 bad request      \n",
            .budget_exhausted => "503 budget exhausted\n",
            .cancelled => "503 cancelled        \n",
            .no_buffer => "503 no buffer        \n",
            .deadline_missed => "408 deadline missed\n",
            .ok, .peer_gone => "500 internal error  \n",
        };
        const status = switch (why) {
            // Exhaustive on purpose, with no `else`. An `else` arm is how
            // `.no_buffer` came to answer "408 Request Timeout": the variant
            // was added to `Ending`, nobody gave it a status, and the fallback
            // quietly claimed the request had timed out. Adding an `Ending`
            // now fails to compile until it has been given an answer.
            //
            // `.ok` and `.peer_gone` do not reach here. `ok` finishes
            // normally and `peer_gone` goes straight to `finish`, which writes
            // nothing because there is nobody to write to. They carry a status
            // anyway rather than `unreachable`, so that being wrong about that
            // shows up as an odd 500 in a log rather than as a panic in a
            // server or as undefined behaviour in a release build.
            .bad_request => "400 Bad Request",
            .budget_exhausted, .cancelled, .no_buffer => "503 Service Unavailable",
            .deadline_missed => "408 Request Timeout",
            .ok, .peer_gone => "500 Internal Server Error",
        };
        // The unwind needs a buffer even if the body could not get one: that
        // is what a cleanup reserve means for memory. The pool holds one back
        // for exactly this.
        //
        // One slot is not always enough. Under a burst every connection fails
        // to acquire in the same reactor pass, so the number of unwinds
        // wanting a buffer at once is the number of connections, and the
        // reserve serves one of them. Measured: 48 connections against a pool
        // of 2 gave 43 `no_buffer` endings and zero delivered 503s, with the
        // reserve in place.
        //
        // So when even the reserve is gone, write the answer straight out. It
        // is short, fixed, and goes to a socket whose send buffer is empty,
        // which is the case where a single write takes all of it. A partial
        // write here loses the tail of an error response on a connection that
        // is closing anyway, and that is a better failure than closing with
        // nothing said.
        if (c.buf.isNull()) {
            c.buf = a.bufs.acquireForCleanupBy(t) orelse {
                var scratch: [128]u8 = undefined;
                const msg = std.fmt.bufPrint(&scratch, "HTTP/1.1 {s}\r\nContent-Length: {d}\r\n\r\n{s}", .{ status, body.len, body }) catch return a.finish(t, c, why);
                _ = sys.write(c.fd, msg.ptr, msg.len);
                return a.finish(t, c, why);
            };
        }
        const b = a.bufs.get(c.buf) orelse return a.finish(t, c, why);
        b.out_sent = 0;
        b.out_len = (std.fmt.bufPrint(&b.out, "HTTP/1.1 {s}\r\nContent-Length: {d}\r\n\r\n{s}", .{ status, body.len, body }) catch
            return a.finish(t, c, why)).len;
        a.s.makeRunnable(t, .spawn);
    }

    fn stepWriting(a: *App, t: TaskId, c: *Conn) void {
        const t0 = nowNs();
        defer a.book.observe(.write, nowNs() - t0);
        const b = a.bufs.get(c.buf) orelse return a.finish(t, c, .peer_gone);
        const rc = sys.write(c.fd, b.out[b.out_sent..].ptr, b.out_len - b.out_sent);
        if (sysErr(rc)) {
            // EAGAIN means wait. EPIPE and ECONNRESET mean the peer is gone,
            // and parking on a dead descriptor holds a task and a buffer that
            // nothing will ever reclaim: it keeps waking on I/O rather than on
            // its deadline, so the deadline check never fires either.
            if (!sys.wouldBlock(rc)) return a.finish(t, c, .peer_gone);
            a.write_stalls += 1;
            return a.park(t, c, .write);
        }
        b.out_sent += rc;
        if (b.out_sent < b.out_len) {
            a.partial_writes += 1;
            return a.park(t, c, .write);
        }

        if (c.phase == .cleanup) return a.finish(t, c, c.ending);

        // keep-alive: reset the request state, KEEP any pipelined bytes.
        // `served` is counted in `stepWorking` now, because one write can
        // carry several answers.
        // The kept bytes already sit at the front of `b.in`, so clear the
        // fields in place rather than round-tripping the payload through
        // scratch space. That scratch was 512 bytes against an `in` of 16 KB,
        // so any client pipelining more than about fifteen requests overran
        // it: a caller-controlled stack write, and nothing in the suite sent
        // a burst big enough to reach it.
        const keep_len = b.in_len;
        b.out_len = 0;
        b.out_sent = 0;
        b.parser = .{};
        b.in_len = keep_len;
        // Reset the request state, keeping the three things that belong to the
        // CONNECTION rather than to the request. This resets by listing what
        // survives, so a new field that has to survive is one nobody has to
        // remember: `tok` did not survive when it was added, every cancel came
        // back stale, and shedding silently did nothing. It cost an afternoon.
        const fd = c.fd;
        const held = c.buf;
        const tok = c.tok;
        c.* = .{ .fd = fd, .buf = held, .tok = tok };

        a.s.setReserve(t, K.cleanup_reserve);
        a.s.renewCap(t, K.work_budget);   // new request, new per-request ceiling
        a.s.arm(t, nowMs() + K.idle_deadline_ms);
        if (keep_len > 0) return a.s.makeRunnable(t, .spawn); // next request already here
        // Nothing pending: give the buffer back before parking. This is the
        // line that makes an idle keep-alive connection cost ~32 bytes.
        a.releaseIfIdle(c);
        a.park(t, c, .read);
    }

    fn finish(a: *App, t: TaskId, c: *Conn, why: Ending) void {
        a.endings[@intFromEnum(why)] += 1;
        if (!a.quiet) std.debug.print("  task {d}: closed [{s}] spent={d} reserve_left={d}\n", .{ t, @tagName(why), c.spent, a.s.reserve[t] });
        a.r.unwatch(t);
        a.r.close(t);
        a.s.disarm(t);
        a.s.release(t);
        // Reclaim on provenance, not on the connection's own bookkeeping.
        // `c.buf` is what this task THINKS it holds; the pool knows what it
        // was actually handed. Those agree on every path that works, and the
        // point of the second one is the paths that do not: an unwind that
        // returns without releasing, or one that never runs. A non-zero count
        // here means somebody left something behind, which is now a number
        // rather than a slow leak nothing could see.
        a.bufs.release(c.buf);
        c.buf = .{};
        const stranded = a.bufs.releaseAllFor(t);
        if (stranded != 0) a.bufs_stranded += stranded;
        sys.close(c.fd);
        a.live_conn[t] = false;
    }
};

var app_storage: App = undefined;

var conn_store_bss: [max_tasks]Conn = undefined;


/// Bind, wire up the scheduler, reactor, buffer pool and supervisor tree, and
/// arm the tick. Returns the port actually bound, so passing 0 lets the
/// operating system choose one.
///
/// Split out from `main` so a test can start a real server and then talk to
/// it. `main` is argv parsing, this, `runUntil`, and the stats dump.
pub fn start(want_port: u16) !u16 {
    sys.ignoreSigpipe();
    const src = sys.tcpSocketNonblock();
    if (sysErr(src)) return error.SocketFailed;
    const sock: i32 = @intCast(src);
    sys.setReuseAddr(sock);
    var addr = sockaddr_in{ .port = sys.hostToNetPort(want_port), .addr = sys.loopback };
    if (sysErr(sys.bind(sock, &addr))) return error.BindFailed;
    if (sysErr(sys.listen(sock, 4096))) return error.ListenFailed;
    _ = sys.getsockname(sock, &addr);

    app_storage = .{ .listener = sock };
    const a = &app_storage;
    a.conn_store = &conn_store_bss;
    a.s.live[listener_task] = true;
    a.s.setPrio(listener_task, prio_listen);            // accept ahead of serving
    try a.r.init();
    a.r.watch(listener_task, sock, .read);

    // Background task: always runnable, budget refilled on a period.
    try a.bufs.init(@intCast(K.io_bufs));
    if (K.drr_quantum < 0) {
        // EXPERIMENTAL, NOT THE DEFAULT. Converges to a fair share and costs
        // an order of magnitude of throughput doing it -- see the note on
        // `Reactor.auto`. A fixed quantum of 1024 is the better operating
        // point today: 31.9% share at 74k req/s, against 48.3% at 6.3k.
        a.r.auto = true;
        a.r.quantum = 4096; // seed; honest rounds correct it
    } else {
        a.r.quantum = K.drr_quantum;
    }
    a.r.service_rounds = K.service_rounds != 0;
    if (false) {
    }
    a.s.grant_size = K.grant;
    a.q.define(sup_root, quota.none, quota.unlimited, .periodic, K.sup_period_ms, "root");
    a.q.define(sup_conn, sup_root, if (K.conn_quota > 0) K.conn_quota else quota.unlimited, .periodic, K.sup_period_ms, "conn");
    a.q.define(sup_bg, sup_root, if (K.bg_budget > 0) K.bg_budget else 0, .periodic, K.bg_period_ms, "bg");
    a.q.define(sup_ctrl, sup_root, quota.unlimited, .periodic, K.sup_period_ms, "ctrl");

    // Control listener on port+1, top priority class.
    const csr = sys.tcpSocketNonblock();
    const csock: i32 = @intCast(csr);
    sys.setReuseAddr(csock);
    var caddr = sockaddr_in{ .port = sys.hostToNetPort(sys.netToHostPort(addr.port) + 1), .addr = sys.loopback };
    _ = sys.bind(csock, &caddr);
    _ = sys.listen(csock, 64);
    a.ctrl_listener = csock;
    a.s.live[ctrl_listener_task] = true;
    a.s.setPrio(ctrl_listener_task, prio_ctrl);
    a.r.watch(ctrl_listener_task, csock, .read);

    _ = a.s.admit(background_task, .{
        .prio = prio_idle,          // only when there is slack
        .quota = sup_bg,
        .cap = quota.unlimited,
        .reserve = 0,               // it never unwinds
    });
    a.s.arm(background_task, nowMs() + K.bg_period_ms);

    var act: posix.Sigaction = .{
        .handler = .{ .handler = onTick },
        .mask = posix.sigemptyset(),
        .flags = posix.SA.RESTART,
    };
    posix.sigaction(.ALRM, &act, null);
    if (K.lazy_tick == 0) {
        armTimer(K.tick_ms);
        a.tick_armed = true;
    }

    std.debug.print("listening on 127.0.0.1:{d}  budget={d} reserve={d} quantum={d}\n", .{
        std.mem.bigToNative(u16, addr.port), K.work_budget, K.cleanup_reserve, K.quantum_units,
    });


    var bound = sockaddr_in{ .port = 0, .addr = 0 };
    if (sysErr(sys.getsockname(app_storage.listener, &bound))) return error.GetSockNameFailed;
    return sys.netToHostPort(bound.port);
}

/// Step until the deadline. The control surface listens on the bound port
/// plus one.
pub fn runUntil(stop_ms: i64) void {
    const a = &app_storage;
    while (nowMs() < stop_ms) a.step();
}

/// One iteration, for a test that wants to interleave rather than block.
pub fn stepOnce() void {
    app_storage.step();
}

/// Live counters, so a test can assert internal consistency rather than only
/// what came back over the socket.
pub const Stats = struct {
    accepted: u64,
    served: u64,
    steps: u64,

    // Named rather than an array, because the two servers do not agree on the
    // `Ending` enum: the io_uring build has an `overloaded` variant the epoll
    // build has no use for. A test that indexes by position would silently
    // read the wrong counter when pointed at the other one.
    ended_ok: u64,
    ended_budget: u64,
    ended_deadline: u64,
    ended_cancelled: u64,
    ended_bad_request: u64,
    ended_no_buffer: u64,
    ended_peer_gone: u64,
    endings_total: u64,

    buf_acquires: u64,
    buf_releases: u64,
    buf_live: usize,
    buf_exhausted: u64,
    top_ups: u64,
    top_up_denials: u64,
    /// Cancels taken, and cancels refused because the token named a task
    /// instance that no longer exists. The second is not an error, but a test
    /// wants to know it stayed at zero when it expected every token to be live.
    cancels: u64,
    cancels_stale: u64,
    partial_writes: u64,
    write_stalls: u64,
    bufs_stranded: usize,
};

pub fn stats() Stats {
    const a = &app_storage;
    var total: u64 = 0;
    for (a.endings) |e| total += e;
    return .{
        .accepted = a.accepted,
        .served = a.served,
        .steps = a.steps,
        .ended_ok = a.endings[@intFromEnum(Ending.ok)],
        .ended_budget = a.endings[@intFromEnum(Ending.budget_exhausted)],
        .ended_deadline = a.endings[@intFromEnum(Ending.deadline_missed)],
        .ended_cancelled = a.endings[@intFromEnum(Ending.cancelled)],
        .ended_bad_request = a.endings[@intFromEnum(Ending.bad_request)],
        .ended_no_buffer = a.endings[@intFromEnum(Ending.no_buffer)],
        .ended_peer_gone = a.endings[@intFromEnum(Ending.peer_gone)],
        .endings_total = total,
        .buf_acquires = a.bufs.acquires,
        .buf_releases = a.bufs.releases,
        .buf_live = a.bufs.live,
        .buf_exhausted = a.bufs.exhausted,
        .top_ups = a.s.top_ups,
        .top_up_denials = a.s.top_up_denials,
        .cancels = a.s.cancels,
        .cancels_stale = a.s.cancels_stale,
        .partial_writes = a.partial_writes,
        .write_stalls = a.write_stalls,
        .bufs_stranded = a.bufs_stranded,
    };
}

/// Knobs, so a test can shrink a budget or a pool before starting.
pub const Tunables = Knobs;
pub fn knobs() *Knobs {
    return &K;
}

pub fn main(init: std.process.Init.Minimal) !void {
    var argv: [24][]const u8 = undefined;
    var argc: usize = 0;
    for (init.args.vector) |x| {
        if (argc == 24) break;
        argv[argc] = std.mem.span(x);
        argc += 1;
    }
    const arg_port: u16 = if (argc > 1) (std.fmt.parseInt(u16, argv[1], 10) catch 0) else 0;
    const arg_secs: i64 = if (argc > 2) (std.fmt.parseInt(i64, argv[2], 10) catch 12) else 12;
    var csv = false;
    for (argv[0..argc]) |arg| {
        if (std.mem.eql(u8, arg, "csv")) { csv = true; continue; }
        const eq = std.mem.indexOfScalar(u8, arg, '=') orelse continue;
        const key = arg[0..eq];
        const val = std.fmt.parseInt(i64, arg[eq + 1 ..], 10) catch continue;
        inline for (@typeInfo(Knobs).@"struct".fields) |f| {
            if (std.mem.eql(u8, key, f.name)) {
                @field(K, f.name) = switch (f.type) {
                    i64 => val,
                    u64 => @intCast(val),
                    usize => @intCast(val),
                    else => @field(K, f.name),
                };
            }
        }
    }

    const port = try start(arg_port);
    _ = port;

    const a = &app_storage;
    const t_start = nowNs();
    const rss_start = rssKb();
    const cpu_start = cpuMs();
    runUntil(nowMs() + arg_secs * 1000);

    const wall_ns = nowNs() - t_start;
    var observed: i64 = 0;
    observed = a.book.observedTotal();
    const unacc = wall_ns - observed;

    if (csv) {
        // One machine-readable line per run. Everything a sweep needs.
        std.debug.print("SRV,{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d}\n", .{
            K.quantum_units, K.work_budget, K.bg_budget, K.bg_period_ms, K.tick_ms,
            a.steps, a.served, a.r.waits, a.bg_iters, a.bg_starved,
            a.ticks_drained, a.tick_coalesced, a.defer_max_ns,
            a.book.ns[@intFromEnum(Label.parse)],
            a.book.ns[@intFromEnum(Label.work)],
            a.book.ns[@intFromEnum(Label.write)],
            unacc,
        });
        return;
    }

    std.debug.print(
        "\nsteps={d} accepted={d} served={d} reactor_waits={d} avg_armed={d} rearms={d}\n",
        .{ a.steps, a.accepted, a.served, a.r.waits, if (a.r.waits > 0) a.r.fds_polled / a.r.waits else 0, a.s.rearms },
    );
    std.debug.print("fair: quantum={d} (auto={}) peak_round={d} q_range={d}..{d} n_max={d} honest={d} rounds={d} throttles={d} resumes={d} bytes={d}\n", .{ a.r.quantum, a.r.auto, a.r.peak_round_bytes, a.r.q_min, a.r.q_max, a.r.n_max, a.r.honest_rounds, a.r.rounds, a.r.throttles, a.r.resumes, a.r.bytes_in });
    std.debug.print("iobufs: cap={d} live={d} high_water={d} acquires={d} releases={d} exhausted={d}  ({d} bytes each)\n", .{
        a.bufs.cap, a.bufs.live, a.bufs.high_water, a.bufs.acquires, a.bufs.releases, a.bufs.exhausted, @sizeOf(iobuf.IoBuf),
    });
    std.debug.print("sizeof(Conn)={d}\n", .{@sizeOf(Conn)});
    std.debug.print("endings ok/budget/deadline/cancel/bad/nobuf/peer = {any}\n", .{a.endings});
    std.debug.print("supervisors: top_ups={d} denials={d}\n", .{ a.s.top_ups, a.s.top_up_denials });
    for (a.q.nodes) |sp| {
        if (sp.quota == 0) continue;
        std.debug.print("  {s:<6} quota={d:<12} granted={d:<14} denials={d}\n", .{ sp.tag, sp.quota, sp.granted, sp.denials });
    }
    std.debug.print("drain_breaks={d}  batched_answers={d} (answers that shared a write)\n", .{ a.drain_breaks, a.batched_answers });
    std.debug.print("rss: start={d}KB end={d}KB   cpu={d}ms of {d}ms wall ({d:.2}%)   asleep={d}ms\n", .{
        rss_start, rssKb(), cpuMs() - cpu_start, @divTrunc(wall_ns, 1_000_000),
        100.0 * @as(f64, @floatFromInt(cpuMs() - cpu_start)) / (@as(f64, @floatFromInt(wall_ns)) / 1e6),
        a.slept_ms,
    });
    std.debug.print("tick: armed {d} disarmed {d} (lazy={d})\n", .{ a.tick_arms, a.tick_disarms, K.lazy_tick });
    std.debug.print("dispatched by class: {any}\n", .{a.s.ran_by_class});
    std.debug.print("background: iters={d} starved={d} (budget {d}/{d}ms)\n", .{ a.bg_iters, a.bg_starved, K.bg_budget, K.bg_period_ms });
    std.debug.print("yield watchdog: yields={d} warnings={d} faults={d} worst_missed_ticks={d} culprit_task={d}\n", .{
        shared.yields_total.load(.monotonic),
        shared.stall_warnings.load(.monotonic),
        shared.stall_faults.load(.monotonic),
        shared.worst_missed.load(.monotonic),
        shared.culprit.load(.monotonic) & 0xfffff,
    });
    std.debug.print("tick: drained={d} coalesced={d} in_kernel={d} in_task={d} max_defer={d:.1}us\n", .{
        a.ticks_drained, a.tick_coalesced,
        shared.took_in_kernel.load(.monotonic), shared.took_in_task.load(.monotonic),
        @as(f64, @floatFromInt(a.defer_max_ns)) / 1000.0,
    });

    std.debug.print("\n{s:<12} {s:>10} {s:>12} {s:>12} {s:>10} {s:>8}\n", .{ "account", "calls", "units", "ns", "ns/unit", "denied" });
    for (std.enums.values(Label)) |acc| {
        const i = @intFromEnum(acc);
        if (a.book.calls[i] == 0 and a.book.ns[i] == 0) continue;
        const npu: f64 = if (a.book.units[i] > 0)
            @as(f64, @floatFromInt(a.book.ns[i])) / @as(f64, @floatFromInt(a.book.units[i]))
        else
            0;
        std.debug.print("{s:<12} {d:>10} {d:>12} {d:>12} {d:>10.1} {d:>8}\n", .{
            @tagName(acc), a.book.calls[i], a.book.units[i], a.book.ns[i], npu, a.book.denied[i],
        });
    }
    std.debug.print("{s:<12} {s:>10} {s:>12} {d:>12}\n", .{ "wall", "", "", wall_ns });
    std.debug.print("{s:<12} {s:>10} {s:>12} {d:>12}   <- charges no units: syscalls, reactor wait, scheduler ({d:.1}%)\n", .{
        "UNACCOUNTED", "", "", unacc,
        100.0 * @as(f64, @floatFromInt(unacc)) / @as(f64, @floatFromInt(wall_ns)),
    });
}
