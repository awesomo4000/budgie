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
const sys = @import("budgie").sys;
const Sched = @import("budgie").sched.Sched;
const TaskId = @import("budgie").sched.TaskId;
const max_tasks = @import("budgie").sched.max_tasks;
const Reactor = @import("budgie").reactor_uring2.Reactor;
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
const Drr = @import("budgie").drr.Drr;

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
    /// 1 = disarm the itimer while blocked, 0 = leave it running always.
    lazy_tick: i64 = 1,
    /// Aggregate cap for the whole connection class, units per sup_period_ms.
    /// 0 = unmetered (the old per-task-only behaviour).
    conn_quota: i64 = 0,
    sup_period_ms: i64 = 100,
    grant: i64 = 1000,
    /// How many connections may have I/O in flight at once. This IS the
    /// memory budget for buffers.
    /// Buffer pool size. Small is better here -- see APIGUIDE. The completion
    /// build acquires only once bytes have arrived, so its high-water mark is
    /// far below the connection count (64 vs 256 measured). The readiness
    /// build is the opposite and needs one per connection.
    io_bufs: i64 = 64,
    /// 1 = append pipelined answers into one buffer and issue a single write
    /// for the run, instead of one write per answer. See the note on the same
    /// knob in app/server.zig.
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
    uring_batch: i64 = 1,
    uring_window_us: i64 = 200,
    fixed_files: i64 = 1,
    fixed_bufs: i64 = 1,
    gen_keys: i64 = 1,
    /// Bytes a connection may receive per DRR round. 0 disables it.
    ///
    /// Byte-fair scheduling across connections. 0 disables it.
    ///
    /// Measured, polite client (32 conns, depth 1) against a greedy one
    /// (32 conns, depth 256), five interleaved trials of each:
    ///
    ///     off : mean 22.0%  median 19.7%  range 14.3-33.5
    ///     on  : mean 59.2%  median 60.3%  range 46.8-68.0
    ///
    /// Non-overlapping ranges. Without it the greedy client takes ~78% of the
    /// machine; with it the polite client gets its share back.
    ///
    /// Note the earlier bistable result (54%/0%/96%) was NOT caused by this --
    /// it was the -ENOBUFS latch in the reactor, which killed connections
    /// silently and produced the same instability with DRR disabled.
    /// Byte fairness. DEFAULT 0 -- OFF.
    ///
    /// It works (greedy client ~95% -> ~38% of requests served) but the metric
    /// it improves turned out not to measure service quality: a client with
    /// 256 requests in flight completes more than one with 1 in flight because
    /// of Little's Law, not because it is starving anyone. The "starved"
    /// client has 48x BETTER median latency and is served in ~0.3ms every time
    /// it asks.
    ///
    /// And it is not free. With fairness on, wrk reports 1-10 non-2xx per run
    /// out of ~660k requests -- throttled connections that went past
    /// `idle_deadline_ms` without service and got a 408. With it off: zero,
    /// every run. Introducing a rare deadline miss to fix a mis-measured
    /// problem is a bad trade.
    ///
    /// Enable it deliberately if you have untrusted tenants and care about
    /// share-of-service as a policy rather than as a proxy for latency.
    drr_quantum: i64 = 0,
    /// 0 = advance on block (works), 1 = advance on service (does NOT --
    /// see drr.zig RoundPolicy). Default 0.
    drr_policy: i64 = 0,
    /// Bytes per kernel recv buffer. Bounds how many pipelined requests can
    /// arrive in ONE completion, so it is the batching knob on the read side.
    uring_buf_size: i64 = 2048,
    /// Completions reaped per wait.
    uring_cqe_batch: i64 = 512,
    /// 0 = plain write(2). 1 = submit the response through the ring.
    ///
    /// DEFAULT 0. Submitting the send removes `write` from the syscall profile
    /// entirely, which looks like a win and is not: the connection then waits
    /// for a `.sent` completion before touching its next request, so it can
    /// process ONE request per io_uring_enter. Measured at pipeline depth 256:
    /// 41,243 req/s and 3.9 requests per enter with the submitted send, versus
    /// 92,573 req/s and 100.0 with plain write(2). A 74ns syscall was traded
    /// for a scheduler round trip.
    async_send: i64 = 0,
    defer_taskrun: i64 = 1,
    coop_taskrun: i64 = 0,
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


const sockaddr_in = extern struct {
    family: u16 = linux.AF.INET,
    port: u16,
    addr: u32,
    zero: [8]u8 = @splat(0),
};

fn sysErr(rc: usize) bool {
    return @as(isize, @bitCast(rc)) < 0;
}

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
};
var shared: Shared = .{};

fn onTick(_: posix.SIG) callconv(.c) void {
    if (shared.in_kernel.load(.monotonic)) {
        _ = shared.took_in_kernel.fetchAdd(1, .monotonic);
    } else {
        _ = shared.took_in_task.fetchAdd(1, .monotonic);
    }
    if (shared.pending.fetchAdd(1, .acq_rel) == 0) {
        shared.arrived_ns.store(nowNs(), .monotonic);
    }
}

const timeval = extern struct { sec: isize, usec: isize };
const itimerval = extern struct { it_interval: timeval, it_value: timeval };

fn armTimer(ms: u64) void {
    const v: itimerval = .{
        .it_interval = .{ .sec = @intCast(ms / 1000), .usec = @intCast((ms % 1000) * 1000) },
        .it_value = .{ .sec = @intCast(ms / 1000), .usec = @intCast((ms % 1000) * 1000) },
    };
    _ = linux.syscall3(.setitimer, 0, @intFromPtr(&v), 0);
}

fn rssKb() u64 {
    var buf: [256]u8 = undefined;
    const fd = linux.open("/proc/self/statm", .{ .ACCMODE = .RDONLY }, 0);
    if (sysErr(fd)) return 0;
    defer _ = linux.close(@intCast(fd));
    const n = linux.read(@intCast(fd), &buf, buf.len);
    if (sysErr(n)) return 0;
    var it = std.mem.tokenizeScalar(u8, buf[0..n], ' ');
    _ = it.next();
    const pages = std.fmt.parseInt(u64, it.next() orelse "0", 10) catch 0;
    return pages * 4; // 4 KiB pages
}

/// utime + stime in milliseconds, from /proc/self/stat.
fn cpuMs() u64 {
    var buf: [1024]u8 = undefined;
    const fd = linux.open("/proc/self/stat", .{ .ACCMODE = .RDONLY }, 0);
    if (sysErr(fd)) return 0;
    defer _ = linux.close(@intCast(fd));
    const n = linux.read(@intCast(fd), &buf, buf.len);
    if (sysErr(n)) return 0;
    const close_paren = std.mem.lastIndexOfScalar(u8, buf[0..n], ')') orelse return 0;
    var it = std.mem.tokenizeScalar(u8, buf[close_paren + 2 .. n], ' ');
    var i: usize = 0;
    var total: u64 = 0;
    while (it.next()) |tok| : (i += 1) {
        if (i == 11 or i == 12) total += std.fmt.parseInt(u64, tok, 10) catch 0;
        if (i > 12) break;
    }
    return total * 10; // USER_HZ = 100
}

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
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, @intCast(ts.sec)) * 1000 + @divTrunc(@as(i64, @intCast(ts.nsec)), 1_000_000);
}

const Phase = enum { reading, working, writing, cleanup };
const Ending = enum { ok, budget_exhausted, deadline_missed, cancelled, bad_request, no_buffer, overloaded, peer_gone };

/// Every mutation of the inbound state, recorded. When the parse fails we
/// dump the last 64 and look for the step where `in_len` dropped by more than
/// `used`, or where `plen` went to zero without a dispatch.
const Trace = struct {
    site: []const u8 = "",
    task: u32 = 0,
    used: u32 = 0,
    in_before: u32 = 0,
    in_after: u32 = 0,
    plen: u32 = 0,
    phase: u8 = 0,
    bufidx: u16 = 0,
};

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
    /// A send is queued in the ring; its completion will advance the write.
    send_inflight: bool = false,
    /// A completion whose bytes did not fit. Its ring buffer is held out of
    /// the ring until we drain, which is what closes the peer's window.
    held_buf: u16 = no_held,
    held_len: u32 = 0,
};

const no_held: u16 = 0xffff;


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
    on_data: u64 = 0,
    completions_handled: u64 = 0,
    data_dropped: u64 = 0,
    reqs_parsed: u64 = 0,
    pipelined_kept: u64 = 0,
    bad_logged: u32 = 0,
    tr: [64]Trace = @splat(.{}),
    tr_n: usize = 0,
    data_stashed: u64 = 0,
    stash_overflow: u64 = 0,
    backpressure_events: u64 = 0,
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
    drr: Drr = .{},
    q: quota.Tree = .{},
    book: acct.Book = .{},
    drr_resumes: u64 = 0,
    ctrl_listener: i32 = -1,
    drain_breaks: u64 = 0,
    endings: [8]u64 = @splat(0),
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

    // ----------------------------------------------------------- the kernel

    /// Completion-mode kernel step.
    ///
    /// Order matters and the first version had it wrong. It was:
    ///     drain runnable -> wait -> handle completions -> END
    /// so every batch of completions sat until the NEXT step, which then woke
    /// one task and waited again. One connection in flight per syscall.
    ///
    /// It is now:
    ///     handle completions -> drain runnable -> (repeat while work exists)
    ///                        -> wait only when genuinely idle
    /// so a batch of completions is fully converted into work, and the work is
    /// fully drained, before the thread ever considers blocking again.
    fn step(a: *App) void {
        a.steps += 1;

        const arrived = shared.arrived_ns.load(.monotonic);
        const drained = shared.pending.swap(0, .acquire);
        if (drained != 0) {
            const lat = nowNs() - arrived;
            if (lat > a.defer_max_ns) a.defer_max_ns = lat;
            a.ticks_drained += 1;
            a.tick_coalesced += drained;
            // Token-bucket refill on the shared clock. One quantum per active
            // flow per tick, so the per-flow rate is quantum/tick regardless
            // of how this backend happens to wake.
            if (K.drr_quantum > 0 and a.drr.policy == .on_tick) {
                var el: [256]TaskId = undefined;
                const nn = a.drr.advanceRound(&el);
                for (el[0..nn]) |et| {
                    if (!a.live_conn[et]) continue;
                    a.drrResume(et);
                }
            }
        }

        // Re-arm any connection whose multishot recv terminated (-ENOBUFS).
        // Without this the connection is silently dead: not runnable, so
        // nothing ever runs to notice.
        {
            var re: [256]TaskId = undefined;
            const n = a.r.takeRearms(&re);
            for (re[0..n]) |rt| {
                if (!a.live_conn[rt]) continue;
                if (a.r.watching(rt)) continue;
                a.r.armRecv(rt, a.conn_store[rt].fd);
            }
        }

        shared.in_kernel.store(false, .release);
        // Inner loop: keep converting completions into work and running it
        // until neither remains. Every pass here is a syscall NOT made.
        var passes: u32 = 0;
        while (passes < 64) : (passes += 1) {
            var did_something = false;

            while (a.r.next()) |comp| {
                if (a.onCompletion(comp)) a.r.release(comp) else a.r.hold(comp);
                did_something = true;
                a.completions_handled += 1;
            }

            while (a.s.popRunnable()) |t| {
                const was_idle = a.s.prio[t] == prio_idle;
                a.run(t);
                did_something = true;
                if (was_idle) break;
                // NOTE: the tick check that used to live here broke the drain.
                // `pending` is cleared once per step, so a tick that fired
                // during the previous block left it set for the whole inner
                // loop and capped every pass at one task. The control-surface
                // bound is preserved by the pass limit above instead.
            }

            if (!did_something) break;
        }
        shared.in_kernel.store(true, .release);

        // Round advance.
        //
        // Advancing only when EVERY active flow is stuck was wrong: with many
        // connections some are always mid-flight, so rounds never advanced and
        // paused flows starved permanently (measured: one client got zero).
        //
        // A round advances whenever anything is waiting on credit, at the
        // point where we are about to block. Every active flow is then
        // credited equally, so all flows see the same round rate and converge
        // to equal shares -- which is the property we want. The absolute rate
        // is self-adjusting: a fast server advances rounds faster.
        if (K.drr_quantum > 0 and a.drr.dueForRound()) {
            var eligible: [256]TaskId = undefined;
            const n = a.drr.advanceRound(&eligible);
            for (eligible[0..n]) |et| {
                if (!a.live_conn[et]) continue;
                const ec = &a.conn_store[et];
                if (!a.r.watching(et)) a.r.armRecv(et, ec.fd);
                a.drr_resumes += 1;
            }
        }

        const now = nowMs();
        a.q.refillPeriodic(now);
        const timeout: i32 = if (a.s.anyRunnable())
            0
        else if (a.s.timeoutMs(now)) |ms|
            @intCast(@min(1000, ms))
        else
            1000;

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

        a.s.expire(nowMs());
    }

    fn run(a: *App, t: TaskId) void {
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
            if (a.s.reasonFor(t) == .deadline) return a.enterCleanup(t, c, .deadline_missed);
        }
        if (c.is_ctrl) return a.stepCtrl(t, c);
        switch (c.phase) {
            .reading => a.stepReading(t, c),
            .working => a.stepWorking(t, c),
            .writing, .cleanup => a.stepWriting(t, c),
        }
    }

    // ------------------------------------------------------------- listener

    fn reAcceptWatch(a: *App) void {
        a.r.watch(listener_task, a.listener, .read);
    }

    fn doAccept(a: *App) void {
        const t0 = nowNs();
        defer a.book.observe(.accept, nowNs() - t0);
        var burst: usize = 0;
        while (burst < K.accept_burst) : (burst += 1) {
            const rc = linux.accept4(a.listener, null, null, linux.SOCK.NONBLOCK);
            if (sysErr(rc)) break;
            const fd: i32 = @intCast(rc);
            if (K.send_buf_bytes > 0) sys.setSendBuf(fd, @intCast(K.send_buf_bytes));
            const t = a.freeTask() orelse {
                _ = linux.close(fd);
                break;
            };
            a.conn_store[t] = .{ .fd = fd };
            a.live_conn[t] = true;
            a.conn_store[t].tok = a.s.admit(t, .{
                .prio = prio_conn,
                .quota = sup_conn,
                .cap = K.work_budget,   // grant arrives via topUp
                .reserve = K.cleanup_reserve,
            });
            if (K.drr_quantum > 0) a.drr.admit(t);
            a.s.arm(t, nowMs() + K.idle_deadline_ms);
            a.r.registerFile(t, fd);
            a.r.armRecv(t, fd);
            a.accepted += 1;
        }
        a.reAcceptWatch();

    }

    /// A CPU-bound task with no I/O at all. It exists to give the tick
    /// something to preempt, and to show that background work throttled by a
    /// refilling budget cannot starve the connections it shares a core with.
    fn stepBackground(a: *App, t: TaskId) void {
        // A refill deadline arrived: top the budget back up. Same wheel, same
        // mechanism as a connection's idle timeout -- only the meaning differs.
        if (a.s.reasonFor(t) == .deadline) {
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
        while (j < K.bg_quantum * 2000) : (j += 1)
            a.bg_sink = a.bg_sink *% 6364136223846793005 +% 1442695040888963407;
        a.bg_iters += 1;
        a.book.observe(.background, nowNs() - t0);
        a.s.makeRunnable(t, .spawn);
    }

    /// The control surface. Echoes whatever arrives, immediately, at the top
    /// priority class. No parsing, no budget, no work phase -- a control
    /// surface that can be made to do arbitrary work is not a control surface.
    fn acceptCtrl(a: *App) void {
        while (true) {
            const rc = linux.accept4(a.ctrl_listener, null, null, linux.SOCK.NONBLOCK);
            if (sysErr(rc)) break;
            const fd: i32 = @intCast(rc);
            const t = a.freeTask() orelse {
                _ = linux.close(fd);
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
        const rc = linux.read(c.fd, &buf, buf.len);
        if (sysErr(rc)) {
            if (!sys.wouldBlock(rc)) {
                a.r.unwatch(t);
                a.s.release(t);
                _ = linux.close(c.fd);
                a.live_conn[t] = false;
                return;
            }
            return a.r.watch(t, c.fd, .read);
        }
        if (rc == 0) {
            a.r.unwatch(t);
            a.s.release(t);
            _ = linux.close(c.fd);
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
            _ = linux.write(c.fd, reply.ptr, reply.len);
        } else {
            _ = linux.write(c.fd, &buf, rc);
        }
        a.r.watch(t, c.fd, .read);
    }

    fn drrResume(a: *App, t: TaskId) void {
        if (!a.r.watching(t)) a.r.armRecv(t, a.conn_store[t].fd);
        a.drr_resumes += 1;
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

    fn park(a: *App, t: TaskId, c: *Conn, i: @import("budgie").reactor_uring2.Interest) void {
        a.r.watch(t, c.fd, i);
    }

    /// Release the buffer when a connection goes idle with nothing pending.
    /// This is the whole saving: a parked keep-alive connection holds none.
    fn releaseIfIdle(a: *App, c: *Conn) void {
        if (c.buf.isNull()) return;
        const b = a.bufs.get(c.buf) orelse return;
        // A partial request lives ONLY in the parser. Releasing the buffer
        // here returns it to the pool and the next completion acquires a
        // fresh one -- so a request split across two recvs lost its first
        // fragment and the remainder failed to parse.
        if (b.in_len != 0 or b.out_sent != b.out_len or !b.parser.isIdle()) return;
        a.rec("release", 0, 0, b.in_len, 0, b.parser.bytesBuffered(), c);
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
    /// In completion mode there is nothing to do when a connection is woken
    /// in the reading phase: bytes arrive through `onCompletion`, not by us
    /// asking. This only re-arms a multishot recv the kernel ended.
    fn stepReading(a: *App, t: TaskId, c: *Conn) void {
        if (a.bufs.get(c.buf)) |b| {
            if (b.in_len > 0 and a.drainIn(t, c, b)) return;
        }
        if (!a.r.watching(t)) a.r.armRecv(t, c.fd);
    }

    /// Returns true if the completion's buffer may go back to the ring.
    /// False means we could not take the bytes, so the buffer stays out and
    /// the kernel stops delivering on that socket -- backpressure.
    fn onCompletion(a: *App, comp: @import("budgie").reactor_uring2.Completion) bool {
        const t = comp.task;
        if (!a.live_conn[t]) return true;
        const c = &a.conn_store[t];
        switch (comp.kind) {
            .eof, .err => {
                a.finish(t, c, .peer_gone);
                return true;
            },
            .writable => {
                a.s.makeRunnable(t, .io);
                return true;
            },
            .sent => {
                a.onSent(t, c, comp.sent);
                return true;
            },
            .data => {
                if (a.onData(t, c, comp.data)) return true;
                if (c.held_buf != no_held) return true; // already holding one
                c.held_buf = comp.buf_id;
                c.held_len = @intCast(comp.data.len);
                a.backpressure_events += 1;
                a.s.makeRunnable(t, .spawn);
                return false;
            },
        }
    }

    /// A send completed. Advance, resubmit the remainder, or finish.
    fn onSent(a: *App, t: TaskId, c: *Conn, res: i32) void {
        const b = a.bufs.get(c.buf) orelse return a.finish(t, c, .peer_gone);
        if (res <= 0) return a.finish(t, c, .peer_gone);
        b.out_sent += @intCast(res);
        c.send_inflight = false;
        if (b.out_sent < b.out_len) {
            a.partial_writes += 1;
            _ = a.r.submitSend(t, c.fd, b.out[b.out_sent..b.out_len]);
            c.send_inflight = true;
            return;
        }
        a.finishWrite(t, c, b);
    }

    /// Bytes in. There is exactly ONE place inbound bytes live -- `b.in` --
    /// and exactly one place they are parsed from. The earlier version
    /// sometimes parsed a raw kernel slice and sometimes a saved stash copy,
    /// and every ordering bug came from the two paths disagreeing about which
    /// bytes came first.
    /// Returns false when the bytes did not fit -- the caller then holds the
    /// ring buffer rather than dropping the data or refusing the request.
    fn onData(a: *App, t: TaskId, c: *Conn, bytes: []const u8) bool {
        a.on_data += 1;
        if (c.buf.isNull()) {
            c.buf = a.bufs.acquire() orelse {
                a.enterCleanup(t, c, .no_buffer);
                return true;
            };
            a.rec("acquire", t, 0, 0, 0, 0, c);
        }
        const b = a.bufs.get(c.buf) orelse {
            a.finish(t, c, .peer_gone);
            return true;
        };

        const room = b.in.len - b.in_len;
        if (bytes.len > room) {
            // Not an error and not a truncation: no room YET. Refusing the
            // ring buffer applies backpressure -- the peer's window closes and
            // the bytes wait in the kernel until we drain. Readiness mode gets
            // this for free because `read` takes only what fits.
            a.stash_overflow += 1;
            // A single completion always fits (see iobuf.IoBuf.in), so
            // reaching here means the connection has accumulated a backlog
            // while busy: the peer is sending faster than we serve. That is a
            // resource limit, not a malformed request.
            a.enterCleanup(t, c, .overloaded);
            return true;
        }
        const before_in = b.in_len;
        @memcpy(b.in[b.in_len..][0..bytes.len], bytes);
        b.in_len += bytes.len;
        a.data_stashed += bytes.len;
        a.rec("onData+", t, bytes.len, before_in, b.in_len, b.parser.bytesBuffered(), c);

        // Charge the byte flow. Bytes are the currency the PEER controls, so
        // this is the only account that can bound a greedy client -- capping
        // compute after the bytes have landed is closing the barn door.
        if (K.drr_quantum > 0 and !a.drr.charge(t, bytes.len)) {
            a.r.pauseRecv(t);
        }

        if (c.phase == .reading) _ = a.drainIn(t, c, b);
        return true;
    }

    /// Parse as much as possible out of `b.in`, consuming exactly what the
    /// parser takes and keeping the rest in place. Returns true if the
    /// connection changed phase.
    fn rec(a: *App, site: []const u8, t: TaskId, used: usize, before: usize, after: usize, plen: usize, c: *Conn) void {
        a.tr[a.tr_n % a.tr.len] = .{
            .site = site, .task = t,
            .used = @intCast(used), .in_before = @intCast(before), .in_after = @intCast(after),
            .plen = @intCast(plen), .phase = @intFromEnum(c.phase), .bufidx = c.buf.idx,
        };
        a.tr_n += 1;
    }

    fn dumpTrace(a: *App) void {
        std.debug.print("  --- last {d} inbound events (site task used in_before->in_after plen phase buf) ---\n", .{@min(a.tr_n, a.tr.len)});
        const n = @min(a.tr_n, a.tr.len);
        var i: usize = if (a.tr_n > a.tr.len) a.tr_n - a.tr.len else 0;
        while (i < a.tr_n) : (i += 1) {
            const e = a.tr[i % a.tr.len];
            std.debug.print("   {s:<12} t={d:<4} used={d:<4} {d:<4}->{d:<4} plen={d:<4} ph={d} buf={d}\n",
                .{ e.site, e.task, e.used, e.in_before, e.in_after, e.plen, e.phase, e.bufidx });
        }
        _ = n;
    }

    /// Return a held buffer once there is room again. Until this happens the
    /// kernel has one fewer buffer and the peer stays throttled.
    fn releaseHeldIfRoom(a: *App, c: *Conn, b: *iobuf.IoBuf) void {
        if (c.held_buf == no_held) return;
        if (b.in.len - b.in_len < c.held_len) return;
        a.r.releaseHeld(c.held_buf);
        c.held_buf = no_held;
        c.held_len = 0;
    }

    fn drainIn(a: *App, t: TaskId, c: *Conn, b: *iobuf.IoBuf) bool {
        const t0 = nowNs();
        defer a.book.observe(.parse, nowNs() - t0);
        while (b.in_len > 0) {
            const before_in = b.in_len;
            const before_p = b.parser.bytesBuffered();
            const used = b.parser.feed(b.in[0..b.in_len]);
            if (used > 0) {
                std.mem.copyForwards(u8, b.in[0 .. b.in_len - used], b.in[used..b.in_len]);
                b.in_len -= used;
                const units = @divTrunc(@as(i64, @intCast(used)), 64) + 1;
                if (!a.chargeAs(t, .parse, units)) {
                    a.enterCleanup(t, c, .budget_exhausted);
                    return true;
                }
            }
            a.rec("drainIn", t, used, before_in, b.in_len, b.parser.bytesBuffered(), c);
            _ = before_p;
            a.releaseHeldIfRoom(c, b);
            switch (b.parser.poll()) {
                .need_input => if (used == 0) return false else continue,
                .protocol_error => |pe| {
                    // Diagnostic only, and behind the same switch as the
                    // per-close line. The serving loop does no I/O of its own:
                    // what it notices goes into counters and `rec`, and the
                    // report happens after `runUntil` returns. A peer that can
                    // make the loop write to a descriptor it does not control
                    // can stall every other connection on it.
                    if (!a.quiet and a.bad_logged < 3) {
                        a.bad_logged += 1;
                        std.debug.print("BAD[{s}] task={d} used={d} in_len={d} buf={d}\n  parser_buf=<{s}>\n", .{
                            @tagName(pe), t, used, b.in_len, c.buf.idx, b.parser.debugBuf(),
                        });
                        a.dumpTrace();
                    }
                    a.enterCleanup(t, c, .bad_request);
                    return true;
                },
                .request => |req| {
                    a.reqs_parsed += 1;
                    c.work_left = req.work_units;
                    // Reset the parser HERE, at the point the request is
                    // dispatched, not later in finishWrite. Its consumed bytes
                    // are already out of b.in, so any state it still holds is
                    // stale by definition -- and if anything reaches drainIn
                    // before finishWrite runs, that stale state gets
                    // concatenated onto the next request and the whole
                    // connection fails to parse.
                    a.rec("dispatch", t, used, b.in_len, b.in_len, b.parser.bytesBuffered(), c);
                    b.parser = .{};
                    c.phase = .working;
                    a.s.makeRunnable(t, .spawn);
                    return true;
                },
            }
        }
        return false;
    }

    /// Feeds buffered bytes to the parser, consuming exactly what it takes and
    /// keeping the rest. Returns true if the connection changed phase.
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

        // Append rather than overwrite: anything already in `out` answers an
        // earlier request on this connection that has not been written yet.
        const answer = std.fmt.bufPrint(b.out[b.out_len..],
            "HTTP/1.1 200 OK\r\nContent-Length: 24\r\n\r\ndone, spent {d:>5} units\n", .{c.spent}) catch
            return a.finish(t, c, .peer_gone);
        b.out_len += answer.len;
        a.served += 1;

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
        const body = switch (why) {
            .bad_request => "400 bad request      \n",
            .overloaded => "503 pipeline too deep\n",
            .budget_exhausted => "503 budget exhausted\n",
            .cancelled => "503 cancelled        \n",
            .no_buffer => "503 no buffer        \n",
            .deadline_missed => "408 deadline missed\n",
            .ok, .peer_gone => "500 internal error  \n",
        };
        const status = switch (why) {
            .bad_request => "400 Bad Request",
            .overloaded => "503 Service Unavailable",
            .budget_exhausted => "503 Service Unavailable",
            .cancelled => "503 Service Unavailable",
            .no_buffer => "503 Service Unavailable",
            .deadline_missed => "408 Request Timeout",
            .ok, .peer_gone => "500 Internal Server Error",
        };
        // The unwind needs a buffer even if the body could not get one: that
        // is what a cleanup reserve means for memory.
        if (c.buf.isNull()) c.buf = a.bufs.acquireForCleanup() orelse return a.finish(t, c, why);
        const b = a.bufs.get(c.buf) orelse return a.finish(t, c, why);
        b.out_sent = 0;
        b.out_len = (std.fmt.bufPrint(&b.out, "HTTP/1.1 {s}\r\nContent-Length: {d}\r\n\r\n{s}", .{ status, body.len, body }) catch
            return a.finish(t, c, why)).len;
        a.s.makeRunnable(t, .spawn);
    }

    fn stepWriting(a: *App, t: TaskId, c: *Conn) void {
        if (c.send_inflight) return; // completion will drive it
        const t0 = nowNs();
        defer a.book.observe(.write, nowNs() - t0);
        const b = a.bufs.get(c.buf) orelse return a.finish(t, c, .peer_gone);
        if (b.out_sent >= b.out_len) return a.finishWrite(t, c, b);
        if (K.async_send == 0) {
            const rc = linux.write(c.fd, b.out[b.out_sent..].ptr, b.out_len - b.out_sent);
            if (sysErr(rc)) {
                // EAGAIN means wait; EPIPE and ECONNRESET mean the peer is
                // gone. Parking on a dead descriptor holds a task and a buffer
                // that nothing reclaims.
                if (!sys.wouldBlock(rc)) return a.finish(t, c, .peer_gone);
                a.write_stalls += 1;
                return a.r.watch(t, c.fd, .write);
            }
            b.out_sent += rc;
            if (b.out_sent < b.out_len) {
                a.partial_writes += 1;
                return a.r.watch(t, c.fd, .write);
            }
            return a.finishWrite(t, c, b);
        }
        if (!a.r.submitSend(t, c.fd, b.out[b.out_sent..b.out_len])) {
            a.s.makeRunnable(t, .spawn); // ring full; retry next pass
            return;
        }
        c.send_inflight = true;
    }

    /// The response is fully out. Either close (cleanup path) or recycle.
    fn finishWrite(a: *App, t: TaskId, c: *Conn, b: *iobuf.IoBuf) void {
        if (c.phase == .cleanup) return a.finish(t, c, c.ending);
        // Reset the parser and the OUTPUT only. `b.in` holds pipelined bytes
        // that belong to the next request and must survive untouched.
        b.out_len = 0;
        b.out_sent = 0;
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
        a.s.renewCap(t, K.work_budget);
        a.s.arm(t, nowMs() + K.idle_deadline_ms);
        // The next request may already be buffered.
        if (b.in_len > 0 and a.drainIn(t, c, b)) return;
        a.releaseIfIdle(c);
        if (!a.r.watching(t) and !a.drr.isThrottled(t)) a.r.armRecv(t, c.fd);
    }

    fn finish(a: *App, t: TaskId, c: *Conn, why: Ending) void {
        a.endings[@intFromEnum(why)] += 1;
        if (!a.quiet) std.debug.print("  task {d}: closed [{s}] spent={d} reserve_left={d}\n", .{ t, @tagName(why), c.spent, a.s.reserve[t] });
        if (c.held_buf != no_held) {
            a.r.releaseHeld(c.held_buf);
            c.held_buf = no_held;
        }
        a.r.unwatch(t);
        a.drr.release(t);
        a.r.unregisterFile(t);
        a.s.disarm(t);
        a.s.release(t);
        a.bufs.release(c.buf);
        c.buf = .{};
        a.r.unregisterFile(t);
        _ = linux.close(c.fd);
        a.live_conn[t] = false;
    }
};

var app_storage: App = undefined;

var conn_store_bss: [max_tasks]Conn = undefined;


/// Bind and wire everything up. Returns the port actually bound, so 0 lets
/// the operating system choose. Split out from `main` for the same reason as
/// in server.zig: so a test can start the real thing and talk to it.
pub fn start(want_port: u16) !u16 {
    sys.ignoreSigpipe();
    const src = linux.socket(linux.AF.INET, linux.SOCK.STREAM | linux.SOCK.NONBLOCK, 0);
    if (sysErr(src)) return error.SocketFailed;
    const sock: i32 = @intCast(src);
    const one: c_int = 1;
    _ = linux.setsockopt(sock, linux.SOL.SOCKET, linux.SO.REUSEADDR, @ptrCast(&one), @sizeOf(c_int));
    var addr = sockaddr_in{ .port = std.mem.nativeToBig(u16, want_port), .addr = 0x0100007f };
    if (sysErr(linux.bind(sock, @ptrCast(&addr), @sizeOf(sockaddr_in)))) return error.BindFailed;
    if (sysErr(linux.listen(sock, 4096))) return error.ListenFailed;
    var len: linux.socklen_t = @sizeOf(sockaddr_in);
    _ = linux.getsockname(sock, @ptrCast(&addr), &len);

    app_storage = .{ .listener = sock };
    const a = &app_storage;
    @import("budgie").reactor_uring2.defer_taskrun = K.defer_taskrun != 0;
    @import("budgie").reactor_uring2.coop_taskrun = K.coop_taskrun != 0;
    try a.r.init(@intCast(K.io_bufs), @intCast(K.uring_buf_size));
    a.r.cqe_batch = @intCast(K.uring_cqe_batch);
    @import("budgie").reactor_uring2.use_gen = K.gen_keys != 0;
    a.r.batch_target = @intCast(K.uring_batch);
    a.drr.quantum = K.drr_quantum;
    a.drr.policy = switch (K.drr_policy) { 0 => .on_block, 2 => .on_tick, else => .on_service };
    a.r.batch_window_ns = K.uring_window_us * 1000;
    a.r.fixed_files = K.fixed_files != 0;
    a.r.fixed_bufs = K.fixed_bufs != 0;
    a.conn_store = &conn_store_bss;
    a.s.live[listener_task] = true;
    a.s.setPrio(listener_task, prio_listen);            // accept ahead of serving
    a.r.watch(listener_task, sock, .read);

    // Background task: always runnable, budget refilled on a period.
    try a.bufs.init(@intCast(K.io_bufs));
    a.s.grant_size = K.grant;
    a.q.define(sup_root, quota.none, quota.unlimited, .periodic, K.sup_period_ms, "root");
    a.q.define(sup_conn, sup_root, if (K.conn_quota > 0) K.conn_quota else quota.unlimited, .periodic, K.sup_period_ms, "conn");
    a.q.define(sup_bg, sup_root, if (K.bg_budget > 0) K.bg_budget else 0, .periodic, K.bg_period_ms, "bg");
    a.q.define(sup_ctrl, sup_root, quota.unlimited, .periodic, K.sup_period_ms, "ctrl");

    // Control listener on port+1, top priority class.
    const csr = linux.socket(linux.AF.INET, linux.SOCK.STREAM | linux.SOCK.NONBLOCK, 0);
    const csock: i32 = @intCast(csr);
    _ = linux.setsockopt(csock, linux.SOL.SOCKET, linux.SO.REUSEADDR, @ptrCast(&one), @sizeOf(c_int));
    var caddr = sockaddr_in{ .port = std.mem.nativeToBig(u16, std.mem.bigToNative(u16, addr.port) + 1), .addr = 0x0100007f };
    _ = linux.bind(csock, @ptrCast(&caddr), @sizeOf(sockaddr_in));
    _ = linux.listen(csock, 64);
    a.ctrl_listener = csock;
    a.s.live[ctrl_listener_task] = true;
    a.s.setPrio(ctrl_listener_task, prio_ctrl);
    a.r.watch(ctrl_listener_task, csock, .read);


    _ = a.s.admit(background_task, .{
        .prio = prio_idle,
        .quota = sup_bg,
        .cap = quota.unlimited,
        .reserve = 0,
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


    var bound: sockaddr_in = .{ .port = 0, .addr = 0 };
    var blen: linux.socklen_t = @sizeOf(sockaddr_in);
    _ = linux.getsockname(app_storage.listener, @ptrCast(&bound), &blen);
    return std.mem.bigToNative(u16, bound.port);
}

pub fn runUntil(stop_ms: i64) void {
    const a = &app_storage;
    while (nowMs() < stop_ms) a.step();
}

/// Same shape as server.zig's, by name rather than by index. This build has an
/// `overloaded` ending the other does not, so positions do not line up.
pub const Stats = struct {
    accepted: u64,
    served: u64,
    steps: u64,

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
    };
}

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
    std.debug.print("data: on_data={d} dropped={d} parsed={d} pipelined_kept={d} stashed={d} deferred={d} backpressure={d} held_now={d}\n", .{ a.on_data, a.data_dropped, a.reqs_parsed, a.pipelined_kept, a.data_stashed, a.stash_overflow, a.backpressure_events, a.r.held });
    std.debug.print("uring feats: NODROP={} FAST_POLL={} EXT_ARG={} CQE_SKIP={} SUBMIT_STABLE={}  fixed_files={} fixed_bufs={} sends={d} file_updates={d}\n", .{
        a.r.feat & linux.IORING_FEAT_NODROP != 0,
        a.r.feat & linux.IORING_FEAT_FAST_POLL != 0,
        a.r.feat & linux.IORING_FEAT_EXT_ARG != 0,
        a.r.feat & linux.IORING_FEAT_CQE_SKIP != 0,
        a.r.feat & linux.IORING_FEAT_SUBMIT_STABLE != 0,
        a.r.fixed_files, a.r.fixed_bufs, a.r.sends, a.r.file_updates,
    });

    // Say it out loud. A run that silently worked around a kernel bug and one
    // that did not are different runs, and the difference belongs in the
    // stats rather than in whoever remembers the kernel version.
    if (@import("budgie").uring_bufring.used_workaround)
        std.debug.print("NOTE: buffer ring needed the inverted-resv workaround -- this kernel has Launchpad #2162843 (Ubuntu 6.8.0-136/-137). Upgrade past it.\n", .{});
    std.debug.print("uring: enters={d} cqes={d} recv_arms={d} multishot_ended={d} rearmed={d} stale_completions={d} enobufs={d} bytes_in={d}\n", .{
        a.r.enters, a.r.cqes_total, a.r.recv_arms, a.r.multishot_reups, a.r.rearms_after_end, a.r.stale_completions, a.r.enobufs, a.r.bytes_in,
    });
    std.debug.print("drr: quantum={d} rounds={d} throttles={d} credits={d} resumes={d} pauses={d} bytes={d}\n", .{
        a.drr.quantum, a.drr.rounds, a.drr.throttles, a.drr.credits, a.drr_resumes, a.r.pauses, a.drr.bytes_charged,
    });
    std.debug.print("iobufs: cap={d} live={d} high_water={d} acquires={d} releases={d} exhausted={d}  ({d} bytes each)\n", .{
        a.bufs.cap, a.bufs.live, a.bufs.high_water, a.bufs.acquires, a.bufs.releases, a.bufs.exhausted, @sizeOf(iobuf.IoBuf),
    });
    std.debug.print("sizeof(Conn)={d}\n", .{@sizeOf(Conn)});
    std.debug.print("endings ok/budget/deadline/cancel/bad/nobuf/overload/peer = {any}\n", .{a.endings});
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
