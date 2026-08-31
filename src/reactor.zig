//! The readiness reactor. Same public surface as the poll version:
//!
//!     watch(task, fd, interest)   unwatch(task)   watching(task)
//!     wait(sched, timeout_ms) -> marks ready tasks runnable
//!
//! Nothing outside this file changed. sched.zig and server.zig are untouched.
//!
//! This file owns the client table, the byte accounting and the round policy,
//! and none of that is platform-specific. The wakeup primitive is, so it lives
//! behind `Backend`: epoll on Linux, kqueue on macOS and the BSDs. The split
//! is deliberately at the smallest possible seam -- arm, disarm, wait, read --
//! because the interesting code here is the ~390 lines of fairness policy
//! below, and having two copies of it that drift apart would be much worse
//! than having two copies of four syscall wrappers.
//!
//! Oneshot arming is what makes the semantics line up exactly. The poll
//! version removed a task from the poll set the moment it became ready, so the
//! set always held only the parked population. Both backends get that from the
//! kernel -- EPOLLONESHOT and EV_ONESHOT -- so `wait` needs no bookkeeping at
//! all on the ready path, and parking costs exactly one registration call.

const std = @import("std");
const builtin = @import("builtin");
const sched = @import("sched.zig");

pub const Interest = @import("interest.zig").Interest;

/// Whether this backend meters bytes per connection and can throttle a greedy
/// one. Declared rather than assumed, so an application can configure DRR only
/// where there is DRR to configure, and a backend without it says so instead
/// of accepting settings it quietly ignores.
pub const has_byte_fairness = true;

/// The platform wakeup primitive. Selected here and nowhere else.
pub const backend = switch (builtin.os.tag) {
    .linux => @import("backend_epoll.zig"),
    .macos, .ios, .tvos, .watchos, .visionos, .freebsd, .netbsd, .openbsd, .dragonfly => @import("backend_kqueue.zig"),
    else => @compileError("no readiness backend for this OS"),
};
const Backend = backend.Backend;

const max_events = 1024;

/// Per-client state the reactor keeps for itself.
///
/// This is the passive-server pattern: a client presents a badge (here, a task
/// id), the server looks it up in ITS OWN table, and acts on that client's
/// behalf. The table is private -- nothing outside this file reads it.
///
/// The byte accounting lived in a shared `drr.zig` that both servers and the
/// scheduler imported, with enforcement split between the reactor (arming) and
/// the application (charging). That is server state with no owner, and it is
/// why six attempts at a portable round policy all failed differently: the two
/// halves could not agree on when a round had elapsed because neither owned
/// the question.
///
/// Here the reactor owns the read, so it owns the bytes, so it owns both the
/// accounting and the arming. Pause and resume cannot race because they are
/// the same code.
const warmup_rounds: u64 = 24;

const Client = struct {
    deficit: i64 = 0,
    throttled: bool = false,
    served_this_round: bool = false,
    backlogged: bool = false,
    want: Interest = .read,
};

pub const Reactor = struct {
    be: Backend = .{},
    /// Whether the fd for this task has ever been added. After the first ADD
    /// every rearm is a MOD, which is what ONESHOT expects.
    added: [sched.max_tasks]bool = @splat(false),
    fd_of: [sched.max_tasks]i32 = @splat(-1),
    armed: usize = 0,

    // --- private client table ---
    client: [sched.max_tasks]Client = @splat(.{}),
    quantum: i64 = 0, // 0 = fairness off
    /// Self-tuning: derive the quantum from observed capacity / active clients
    /// instead of taking a fixed number.
    ///
    /// A fixed quantum is a RATE CAP, not a share: every client is capped at
    /// quantum/tick, so it is fair only if that number happens to be near
    /// capacity/N. Too high and the greedy client is not capped at all (37.7%
    /// polite share at 1024); too low and everyone is capped and throughput
    /// suffers (47.5% at 128, and falling).
    ///
    /// Capacity is estimated as a PEAK HOLD with slow decay, not an average.
    /// That matters: throttling reduces observed throughput, so an average
    /// would feed its own suppression back into the estimate and spiral down.
    /// Capping can never raise the peak, so peak-hold is stable under
    /// self-throttling.
    /// STATUS: converges, and is not usable yet.
    ///
    /// It reaches a fair share (48.3%, stable to 0.1 across trials) and pays
    /// 15x throughput for it: 6.3k req/s against 97.8k unthrottled and 74k at
    /// a fixed quantum of 1024. The share is right and the operating point is
    /// wrong.
    ///
    /// The cause is work conservation, not the estimator. Every client is
    /// credited `quantum` per tick whether it wants it or not, and unused
    /// allowance is discarded rather than redistributed -- so a polite client
    /// leaves most of its share on the floor and nobody picks it up. Dividing
    /// by the demanding set instead of every client is an attempt at that and
    /// did not move the number, so the redistribution has to happen WITHIN a
    /// round, not by adjusting the divisor between rounds.
    ///
    /// Three estimator bugs were fixed getting this far, each measured:
    ///   * peak-hold on a bursty signal latched 10-30x above the mean
    ///     (quantum 1670-6920 where ~170 binds)
    ///   * an EWMA of throughput you are limiting is a closed loop; it walked
    ///     to the floor and collapsed throughput
    ///   * the estimate starts at zero, so the first round clamps the quantum
    ///     to the floor and no honest sample is ever taken again -- needs a
    ///     warmup before it is trusted
    auto: bool = false,
    peak_round_bytes: i64 = 0,
    round_bytes: i64 = 0,
    round_clients: usize = 0,
    /// Open clients. THIS is the fair-share denominator, not the number that
    /// happened to read in the last tick -- a client waiting for its turn is
    /// still competing. Dividing by the per-round reader count gave a quantum
    /// 60x too large (11065 vs the ~173 that actually binds).
    n_clients: usize = 0,
    q_min: i64 = 1 << 40,
    q_max: i64 = 0,
    n_max: usize = 0,
    throttled_this_round: usize = 0,
    honest_rounds: u64 = 0,
    /// Clients throttled at least once in the recent past: the ones that want
    /// more than their current share.
    demanding: usize = 0,

    // --- service-counted rounds, with the backend supplying the backlogged set ---
    //
    // The whole difficulty with DRR here was that classic DRR observes the
    // DEQUEUE -- "a round ends once every backlogged flow has had a turn" --
    // and we appeared to control only the arrival. Every round condition
    // phrased in terms of service either fired per charge or deadlocked,
    // because a throttled flow stops being served and therefore stops looking
    // backlogged.
    //
    // But the backend already reports exactly the backlogged set: a client with
    // nothing pending is simply not in it. And a client we refused to arm is
    // known to have data waiting, because that is why we refused. Together
    // those give a backlogged count that SURVIVES throttling, which is the
    // piece that was missing.
    //
    // A round then ends when every backlogged client has been read from once.
    // Work conservation is automatic: an idle client is not in the set, so it
    // neither takes a turn nor dilutes anyone else's.
    backlogged: usize = 0,
    served_this_round: usize = 0,
    service_rounds: bool = false,
    min_quantum: i64 = 64,
    max_quantum: i64 = 1 << 20,
    n_throttled: usize = 0,
    throttles: u64 = 0,
    resumes: u64 = 0,
    rounds: u64 = 0,
    bytes_in: u64 = 0,

    waits: u64 = 0,
    fds_polled: u64 = 0, // kept for stat parity: counts armed fds at wait time
    ctls: u64 = 0,

    pub fn init(r: *Reactor) !void {
        try r.be.init();
    }

    pub fn deinit(r: *Reactor) void {
        r.be.deinit();
    }

    /// Admit a client. Its deficit starts full.
    pub fn open(r: *Reactor, task: sched.TaskId) void {
        r.client[task] = .{ .deficit = r.quantum };
        r.n_clients += 1;
    }

    pub fn close(r: *Reactor, task: sched.TaskId) void {
        if (r.client[task].throttled and r.n_throttled > 0) r.n_throttled -= 1;
        if (r.n_clients > 0) r.n_clients -= 1;
        r.client[task] = .{};
    }

    /// Read on the client's behalf, and charge it.
    ///
    /// The reactor does the syscall so that it sees the bytes. That is the
    /// whole point: a readiness reactor that hands out `watch` and lets the
    /// caller read has no idea how much anyone consumed, so fairness has to
    /// live somewhere else and coordinate -- which is exactly what did not
    /// work.
    pub fn read(r: *Reactor, task: sched.TaskId, fd: i32, buf: []u8) isize {
        const n = Backend.read(fd, buf);
        if (n <= 0) return n;
        r.bytes_in += @intCast(n);
        r.round_bytes += n;
        if (r.quantum > 0) {
            const c = &r.client[task];
            if (!c.served_this_round) {
                c.served_this_round = true;
                r.round_clients += 1;
                r.served_this_round += 1;
            }
            c.deficit -= n;
            if (c.deficit <= 0 and !c.throttled) {
                c.throttled = true;
                r.n_throttled += 1;
                r.throttles += 1;
                r.throttled_this_round += 1;
            }
        }
        return n;
    }

    /// Arm interest. A throttled client is simply not armed -- the socket
    /// stays unread, the window closes, the peer stalls. The caller does not
    /// need to know; it asks for what it wants and the server decides.
    pub fn watch(r: *Reactor, task: sched.TaskId, fd: i32, i: Interest) void {
        if (r.quantum > 0 and i == .read and r.client[task].throttled) {
            r.client[task].backlogged = true;
            r.client[task].want = i;
            r.fd_of[task] = fd;
            return; // deliberately not armed
        }
        r.armNow(task, fd, i);
    }

    /// Credit every throttled client and arm the ones that can proceed.
    ///
    /// Called by the loop when it is about to block. Because the reactor owns
    /// both halves, "credited" and "armed" happen in the same instant and
    /// there is no window for a resume to race its own pause.
    pub fn advanceRound(r: *Reactor) usize {
        if (r.quantum == 0) return 0;
        r.rounds += 1;

        if (r.auto) {
            // EWMA of bytes per round, not a peak.
            //
            // Peak-hold was chosen to avoid a feedback spiral -- throttling
            // lowers throughput, which would lower an average, which would
            // tighten the quantum further. But the signal is bursty: one busy
            // round set the peak 10-30x above the mean and the quantum came
            // out at 1670-6920 when ~170 is what actually binds.
            //
            // The spiral does not materialise because the quantum is a FLOOR
            // on every client equally: if it drops too far, all clients are
            // capped, throughput plateaus rather than collapsing, and
            // `min_quantum` bounds the bottom.
            // Only sample rounds in which WE THROTTLED NOBODY.
            //
            // Estimating capacity from throughput you are limiting is a closed
            // loop, and it does exactly what a closed loop does: the EWMA fed
            // its own suppression back in, the quantum walked to the floor,
            // and throughput collapsed 100k -> 12k while the share looked
            // beautiful at 45%. An unthrottled round is demand-limited, so it
            // is the only honest sample of what the system can absorb.
            //
            // Under sustained overload every round throttles and the estimate
            // FREEZES at its last honest value -- which is the pre-overload
            // capacity, and is the number we want.
            if (r.throttled_this_round == 0) {
                r.peak_round_bytes += @divTrunc(r.round_bytes - r.peak_round_bytes, 8);
                r.honest_rounds += 1;
            }
            // Warm up before trusting the estimate. It starts at zero, and
            // zero divided by anything clamps to the floor -- which throttles
            // everyone on round one, so no honest sample is ever taken again
            // and the estimate is frozen at nothing. Hold the seed quantum
            // until enough unthrottled rounds have been observed.
            if (r.honest_rounds >= warmup_rounds) {
                // Denominator is the DEMANDING clients, not all of them.
                //
                // Dividing capacity by every open connection gives each one an
                // equal slice whether it wants it or not, and unused slices are
                // discarded rather than redistributed -- which is exactly the
                // work conservation max-min fairness is supposed to have.
                // Measured cost: fairness bought at 6x throughput (47.6% share
                // at 12.5k req/s, against 29.9% at 71.9k).
                //
                // A client that has been throttled recently is one that wants
                // more than it is getting. Only those should divide the pie.
                const demanders: i64 = @intCast(@max(1, r.demanding));
                const n: i64 = demanders;
                const q = @divTrunc(r.peak_round_bytes, n);
                r.quantum = @max(r.min_quantum, @min(r.max_quantum, q));
            }
            if (r.n_clients > 1) { // ignore teardown, when n collapses to 1
                if (r.quantum < r.q_min) r.q_min = r.quantum;
                if (r.quantum > r.q_max) r.q_max = r.quantum;
                if (r.n_clients > r.n_max) r.n_max = r.n_clients;
            }
        }
        // Decay the demanding set toward the currently-throttled count, so a
        // client that stops wanting more drops out within a few rounds.
        if (r.n_throttled > r.demanding) {
            r.demanding = r.n_throttled;
        } else if (r.demanding > r.n_throttled) {
            r.demanding -= (r.demanding - r.n_throttled + 7) / 8;
        }
        r.round_bytes = 0;
        r.round_clients = 0;
        r.throttled_this_round = 0;
        r.served_this_round = 0;
        for (&r.client) |*c| c.served_this_round = false;

        if (r.n_throttled == 0) return 0;
        var woke: usize = 0;
        for (&r.client, 0..) |*c, i| {
            if (!c.throttled) continue;
            c.deficit = @min(c.deficit + r.quantum, r.quantum);
            if (c.deficit <= 0) continue;
            c.throttled = false;
            r.n_throttled -= 1;
            if (c.backlogged) {
                c.backlogged = false;
                r.armNow(@intCast(i), r.fd_of[i], c.want);
                woke += 1;
                r.resumes += 1;
            }
        }
        return woke;
    }

    pub fn throttledCount(r: *const Reactor) usize {
        return r.n_throttled;
    }

    /// Has every backlogged client had its turn?
    ///
    /// STATUS: implemented, measured, DOES NOT WORK. Default off.
    ///
    /// The idea was that the backend already reports the backlogged set, giving
    /// the service-counted round that classic DRR needs. Two failures:
    ///
    ///   * counting throttled clients as backlogged deadlocks -- they are
    ///     exactly the ones we refuse to serve, so the count never completes.
    ///     8 rounds in 5 seconds, everything stalled.
    ///   * counting only the ready set gives no fairness at all (3.5-6.7%
    ///     share against 7.2% with fairness off). The ready set is "who has
    ///     data at this instant", not "who is competing": with 64 clients and
    ///     fast service only a handful are ready at once, so a round completes
    ///     after a few reads and credits everyone almost continuously. Round
    ///     rate is emergent from the wake pattern again -- the same root cause
    ///     one level down.
    ///
    /// What is actually needed is a notion of "competing" that spans time,
    /// not an instantaneous readiness snapshot.
    pub fn roundComplete(r: *const Reactor) bool {
        if (!r.service_rounds) return false;
        return r.backlogged > 0 and r.served_this_round >= r.backlogged;
    }

    fn armNow(r: *Reactor, task: sched.TaskId, fd: i32, i: Interest) void {
        r.ctls += 1;
        if (!r.be.arm(task, fd, i, r.added[task])) return;
        if (!r.added[task]) {
            r.added[task] = true;
            r.fd_of[task] = fd;
        }
        r.armed += 1;
    }

    pub fn unwatch(r: *Reactor, task: sched.TaskId) void {
        if (!r.added[task]) return;
        r.ctls += 1;
        r.be.disarm(task, r.fd_of[task]);
        r.added[task] = false;
        r.fd_of[task] = -1;
        if (r.armed > 0) r.armed -= 1;
    }

    pub fn watching(r: *const Reactor, task: sched.TaskId) bool {
        return r.added[task];
    }

    /// Block up to `timeout_ms` and mark every ready task runnable.
    ///
    /// The kernel hands back only the ready set, so this is O(ready) rather
    /// than O(registered). That is the entire difference from the poll build.
    pub fn wait(r: *Reactor, s: *sched.Sched, timeout_ms: i32) usize {
        var ready: [max_events]sched.TaskId = undefined;
        r.waits += 1;
        r.fds_polled += r.armed;
        const n = r.be.wait(&ready, timeout_ms);
        // The ready set is the set we WILL serve this round.
        //
        // Adding the throttled clients to it deadlocks: they are precisely the
        // ones we refuse to serve, so `served >= backlogged` can never hold and
        // rounds stop (measured: 8 rounds in 5s, everything stalled). They get
        // credited when the round ends, not counted as participants in it.
        r.backlogged = n;
        for (ready[0..n]) |task| {
            // ONESHOT already disarmed it kernel-side; mirror that locally so
            // the next park issues a MOD rather than a redundant ADD.
            if (r.armed > 0) r.armed -= 1;
            s.makeRunnable(task, .io);
        }
        return n;
    }
};
