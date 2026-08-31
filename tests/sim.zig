//! Deterministic simulation of the real scheduler.
//!
//! This imports `sched.zig` unmodified -- no test hooks, no seams cut for the
//! occasion. That is possible because every time-taking function in the
//! scheduler already takes `now` as a parameter, so the only thing this file
//! has to replace is the clock and the source of I/O readiness.
//!
//! Three properties are checked:
//!
//!   1. REPLAY      same seed twice -> byte-identical trace hash.
//!   2. SENSITIVITY change one budget -> a different hash, still reproducible.
//!                  (A hash that never changes is not proving anything.)
//!   3. TIME TRAVEL virtual time jumps to the next scheduled event, so hours
//!                  of deadline behaviour run in milliseconds.
//!
//! Run G exists for none of those. The timer wheel is 4096 slots of 1ms, and
//! every deadline either server arms is 3000ms, so nothing here had ever been
//! armed past one revolution and the overflow list was dead code under test. G
//! sets the deadline to 10s and reaches it a hundred thousand times.
//!
//! usage: sim <seed> <tasks> <virtual_seconds> [conn_quota] [work_budget]

const std = @import("std");
const sched = @import("budgie").sched;
const Clock = @import("budgie").clock.Clock;
const quota = @import("budgie").quota;
/// Wall time, used only to report how long the run took. The simulation
/// itself never reads this -- it runs on the virtual clock, which is the
/// whole point -- so this is a stopwatch and nothing branches on it.
const wallNs = @import("budgie").clock.monotonicNs;

const Sched = sched.Sched;
const TaskId = sched.TaskId;

const sup_root: quota.Id = 0;
const sup_conn: quota.Id = 1;

/// Something that happens to a task at a virtual time. `.io` stands in for the
/// reactor; `.cancel` stands in for whoever holds the token deciding to use it.
const Event = struct {
    at_ns: i64,
    task: TaskId,
    kind: enum { io, cancel } = .io,
    /// The token the holder had when they scheduled this. Carried rather than
    /// looked up, because looking it up would always find the CURRENT
    /// instance and a cancel would never be stale. Holding a token across the
    /// death of the thing it names is the case the generation counter exists
    /// for, so the sim has to be able to express it.
    tok: sched.CancelTok = .none,
};

const Phase = enum { reading, working, writing, parked };

const Task = struct {
    phase: Phase = .parked,
    work_left: i64 = 0,
    served: u64 = 0,
    ended: u64 = 0,
    /// Whether this slot currently holds an admitted task. The sim admits,
    /// releases and re-admits, so this is what conservation is counted against.
    live: bool = false,
};

const Sim = struct {
    s: Sched = .{},
    q: quota.Tree = .{},
    clock: Clock,
    rng: std.Random.DefaultPrng,
    tasks: [512]Task = @splat(.{}),
    /// The right to cancel each task instance, from admission. There is no
    /// other way to get one, which is the point.
    toks: [512]sched.CancelTok = @splat(.{}),
    n_tasks: u32,

    /// Pending I/O, kept sorted by time. Small and linear on purpose: a
    /// simulation is allowed to be slow, it is not allowed to be nondeterministic.
    ev: [4096]Event = undefined,
    ev_len: usize = 0,

    /// Fold of every scheduling decision. Two runs agree iff they made the
    /// same decisions in the same order at the same virtual times.
    trace: u64 = 0xcbf29ce484222325,
    steps: u64 = 0,
    dispatches: u64 = 0,
    deadline_wakes: u64 = 0,
    budget_denials: u64 = 0,
    admits: u64 = 0,
    cancels_taken: u64 = 0,
    cancels_stale: u64 = 0,
    unwinds: u64 = 0,
    /// The first thing the scheduler said about itself that was not true, and
    /// how far in. Kept rather than panicked on, so a run reports where it
    /// went wrong instead of dying at the first sign of it.
    violation: ?@import("budgie").invariant.Violation = null,
    violated_at: u64 = 0,

    fn mix(sm: *Sim, a: u64) void {
        sm.trace ^= a;
        sm.trace *%= 0x100000001b3;
    }

    fn pushEvent(sm: *Sim, at_ns: i64, task: TaskId) void {
        sm.push(.{ .at_ns = at_ns, .task = task, .kind = .io });
    }

    fn push(sm: *Sim, e: Event) void {
        if (sm.ev_len == sm.ev.len) return;
        var i = sm.ev_len;
        while (i > 0 and sm.ev[i - 1].at_ns > e.at_ns) : (i -= 1) sm.ev[i] = sm.ev[i - 1];
        sm.ev[i] = e;
        sm.ev_len += 1;
    }

    fn nextEventNs(sm: *const Sim) ?i64 {
        if (sm.ev_len == 0) return null;
        return sm.ev[0].at_ns;
    }

    fn popDue(sm: *Sim, now_ns: i64) ?Event {
        if (sm.ev_len == 0 or sm.ev[0].at_ns > now_ns) return null;
        const e = sm.ev[0];
        std.mem.copyForwards(Event, sm.ev[0 .. sm.ev_len - 1], sm.ev[1..sm.ev_len]);
        sm.ev_len -= 1;
        return e;
    }

    /// Ask the scheduler whether it is still telling the truth. Every so many
    /// dispatches rather than every one: `check` walks every task slot, and at
    /// half a million dispatches per run that would dominate. Every 64 is
    /// often enough to catch a state that persists, which is what a broken
    /// invariant does.
    fn audit(sm: *Sim) void {
        if (sm.violation != null) return;
        if (sm.s.check()) |v| {
            sm.violation = v;
            sm.violated_at = sm.dispatches;
        }
    }

    /// Admit a task and keep the token. The only place a token comes from.
    ///
    /// `staggered` is for the opening loop only, where every task is admitted
    /// before anything runs and the arrival times are spread out to avoid a
    /// thundering herd at t=0. There, and only there, the ring holds exactly
    /// the task just admitted, so consuming its spawn wake takes that task.
    ///
    /// Doing the same thing for a recycled slot was a bug, and the invariant
    /// found it: `popRunnable` pops the highest-priority FIFO entry, which
    /// mid-run is somebody else, so it discarded an unrelated task's dispatch
    /// and then cleared `queued` for a task still sitting in a ring. Four
    /// seeds in ten reported "everything in a ring is marked queued". For a
    /// recycled slot, admission IS the arrival, which is also what a server
    /// does: it admits a connection because one turned up.
    fn admitTask(sm: *Sim, t: TaskId, cfg: Cfg, r: std.Random, staggered: bool) void {
        sm.toks[t] = sm.s.admit(t, .{
            .prio = 2,
            .quota = sup_conn,
            .cap = cfg.cap,
            .reserve = cfg.reserve,
        });
        sm.tasks[t] = .{ .live = true };
        sm.admits += 1;
        if (staggered) {
            _ = sm.s.popRunnable(); // safe here: the ring holds only this task
            sm.s.queued[t] = false;
            sm.pushEvent(sm.clock.v_ns + r.intRangeAtMost(i64, 0, 50_000_000), t);
        }
        sm.s.arm(t, sm.clock.ms() + cfg.idle_deadline_ms);
        if (cfg.cancel_permille > 0 and r.uintLessThan(u32, 1000) < cfg.cancel_permille) {
            // Somebody decides, at some point, to stop this one. Scheduled
            // from the same rng as everything else, so replay still holds.
            sm.push(.{
                .at_ns = sm.clock.v_ns + r.intRangeAtMost(i64, 1_000_000, 400_000_000),
                .task = t,
                .kind = .cancel,
                .tok = sm.toks[t],
            });
        }
    }

    /// What the servers do in `unwind`: spend the reserve, give the slot back,
    /// and let a new connection take it later. The body budget is zero by now,
    /// which is the whole point of the reserve being a separate number.
    fn unwind(sm: *Sim, t: TaskId, cfg: Cfg, r: std.Random) void {
        sm.unwinds += 1;
        sm.s.chargeReserve(t, cfg.unwind_cost);
        sm.tasks[t].ended += 1;
        sm.tasks[t].live = false;
        sm.s.release(t);
        // A new connection arrives on the same slot after a while, which is
        // what exercises admission, generation bumping, and any token still
        // held for the instance that just went away.
        sm.push(.{
            .at_ns = sm.clock.v_ns + r.intRangeAtMost(i64, 1_000_000, 50_000_000),
            .task = t,
            .kind = .io,
        });
        // And the holder tries again later with the token they still have, for
        // an instance that no longer exists. It should be a counted no-op and
        // must not touch whoever inherits the slot.
        sm.push(.{
            .at_ns = sm.clock.v_ns + r.intRangeAtMost(i64, 60_000_000, 300_000_000),
            .task = t,
            .kind = .cancel,
            .tok = sm.toks[t],
        });
    }

    // ---------------------------------------------------------- the model

    fn run(sm: *Sim, until_ns: i64, cfg: Cfg) void {
        const r = sm.rng.random();

        // Every task starts parked with a request arriving at a random time.
        for (1..sm.n_tasks + 1) |i| sm.admitTask(@intCast(i), cfg, r, true);

        while (sm.clock.v_ns < until_ns) {
            sm.steps += 1;

            // 1. Drain runnable at the current virtual instant.
            while (sm.s.popRunnable()) |t| {
                sm.dispatches += 1;
                sm.mix(@as(u64, t) << 32 | @intFromEnum(sm.s.reasonFor(t)));
                sm.mix(@bitCast(sm.clock.v_ns));
                // Ask the runtime before running the task, exactly as both
                // servers now do. A task that has had its authority withdrawn
                // never gets to decide anything about it.
                if (sm.s.faultOf(t)) |f| {
                    sm.mix(@intFromEnum(f));
                    sm.unwind(t, cfg, r);
                } else {
                    sm.step(t, cfg, r);
                }
                if (sm.dispatches % 64 == 0) sm.audit();
            }

            // 2. Jump to whichever comes first: scripted I/O, a deadline, or
            //    a supervisor refill. This is the whole point of virtual time
            //    -- idle intervals cost nothing.
            const now_ms = sm.clock.ms();
            var next_ns: i64 = until_ns;
            if (sm.nextEventNs()) |e| next_ns = @min(next_ns, e);
            if (sm.s.timeoutMs(now_ms)) |d| next_ns = @min(next_ns, sm.clock.v_ns + d * 1_000_000);
            if (sm.q.nextRefillMs(now_ms)) |d| next_ns = @min(next_ns, sm.clock.v_ns + d * 1_000_000);
            if (next_ns <= sm.clock.v_ns) next_ns = sm.clock.v_ns + 1_000_000;
            sm.clock.advanceTo(next_ns);

            // 3. Deliver everything now due.
            while (sm.popDue(sm.clock.v_ns)) |e| switch (e.kind) {
                .io => if (sm.tasks[e.task].live)
                    sm.s.makeRunnable(e.task, .io)
                else
                    // The slot was released; this is the next connection
                    // arriving on it.
                    sm.admitTask(e.task, cfg, r, false),
                .cancel => {
                    // Through the token, which is the only way. If the task
                    // instance it names has already gone, this is a counted
                    // no-op rather than a cancel of whoever inherited the slot,
                    // and the sim provokes that deliberately by recycling
                    // slots while tokens are outstanding.
                    if (sm.s.cancel(e.tok)) sm.cancels_taken += 1 else sm.cancels_stale += 1;
                    sm.mix(0xca7ce1 ^ @as(u64, e.task));
                },
            };
            const nm = sm.clock.ms();
            sm.q.refillPeriodic(nm);
            const before = sm.s.fires;
            sm.s.expire(nm);
            sm.deadline_wakes += sm.s.fires - before;
        }
        sm.audit();
    }

    fn step(sm: *Sim, t: TaskId, cfg: Cfg, r: std.Random) void {
        const tk = &sm.tasks[t];

        if (sm.s.isExpired(t)) {
            // Idle timeout: end the request, re-arm, wait for the next arrival.
            tk.ended += 1;
            tk.phase = .parked;
            sm.s.arm(t, sm.clock.ms() + cfg.idle_deadline_ms);
            sm.pushEvent(sm.clock.v_ns + r.intRangeAtMost(i64, 1_000_000, 200_000_000), t);
            return;
        }

        switch (tk.phase) {
            .parked, .reading => {
                if (!sm.s.charge(t, &sm.q, 1)) {
                    sm.budget_denials += 1;
                    tk.phase = .parked;
                    sm.pushEvent(sm.clock.v_ns + 10_000_000, t);
                    return;
                }
                tk.work_left = r.intRangeAtMost(i64, 0, cfg.max_work);
                tk.phase = .working;
                sm.s.makeRunnable(t, .spawn);
            },
            .working => {
                const charge = @min(cfg.quantum, tk.work_left);
                if (charge > 0 and !sm.s.charge(t, &sm.q, charge)) {
                    sm.budget_denials += 1;
                    tk.phase = .parked;
                    // Re-arm without disarming first, which is what both
                    // servers do -- `arm` unlinks and relinks by contract. The
                    // extra `disarm` that used to be here hid a bug: it was
                    // the only caller that took an entry off the overflow list
                    // before re-arming, so `arm`'s own handling of that case
                    // was never exercised, and it was wrong.
                    sm.s.arm(t, sm.clock.ms() + cfg.idle_deadline_ms);
                    sm.pushEvent(sm.clock.v_ns + 5_000_000, t);
                    return;
                }
                tk.work_left -= charge;
                // Virtual cost of the quantum: units, converted at a fixed rate.
                sm.clock.advanceBy(charge * cfg.ns_per_unit);
                if (tk.work_left > 0) return sm.s.makeRunnable(t, .spawn);
                tk.phase = .writing;
                sm.s.makeRunnable(t, .spawn);
            },
            .writing => {
                tk.served += 1;
                tk.phase = .parked;
                sm.s.setReserve(t, cfg.reserve);
                sm.s.renewCap(t, cfg.cap);
                sm.s.arm(t, sm.clock.ms() + cfg.idle_deadline_ms);
                sm.pushEvent(sm.clock.v_ns + r.intRangeAtMost(i64, 500_000, 30_000_000), t);
            },
        }
    }
};

const Cfg = struct {
    quantum: i64 = 250,
    max_work: i64 = 900,
    reserve: i64 = 50,
    cap: i64 = 1000,
    idle_deadline_ms: i64 = 3000,
    ns_per_unit: i64 = 40,
    conn_quota: i64 = 0,
    grant: i64 = 1000,
    /// How many tasks in a thousand get cancelled at some point after they are
    /// admitted. 0 leaves the whole mechanism unexercised, which is what every
    /// run here did until now.
    cancel_permille: u32 = 0,
    /// What one unwind spends out of the reserve. Larger than `reserve` would
    /// overrun it, which `Sched.check` now notices.
    unwind_cost: i64 = 10,
};

/// `Sim` embeds a whole `Sched` (~0.6 MB), so six of them will not fit on an
/// 8 MB stack. Static storage; the simulation is single-threaded by design.
var sims: [9]Sim = undefined;

fn once(slot: usize, seed: u64, n_tasks: u32, virt_s: i64, cfg: Cfg) *Sim {
    const sm = &sims[slot];
    sm.* = Sim{
        .clock = Clock.virtualAt(1_000_000_000),
        .rng = std.Random.DefaultPrng.init(seed),
        .n_tasks = n_tasks,
    };
    sm.s.grant_size = cfg.grant;
    sm.q.define(sup_root, quota.none, quota.unlimited, .periodic, 100, "root");
    sm.q.define(sup_conn, sup_root, if (cfg.conn_quota > 0) cfg.conn_quota else quota.unlimited, .periodic, 100, "conn");
    sm.run(1_000_000_000 + virt_s * 1_000_000_000, cfg);
    return sm;
}

fn served(sm: *const Sim) u64 {
    var n: u64 = 0;
    for (sm.tasks) |t| n += t.served;
    return n;
}

pub fn main(init: std.process.Init.Minimal) !void {
    // Iterate the arguments rather than walking `init.args.vector` directly.
    // That field is a POSIX shape: on Windows it is UTF-16 code units of the
    // raw command line, so `std.mem.span` on it is a compile error rather than
    // a runtime surprise. `iterateAllocator` is the form that works on all
    // three, and costs an allocator the simulation does not otherwise need.
    var it = try init.args.iterateAllocator(std.heap.page_allocator);
    defer it.deinit();
    _ = it.skip(); // program name
    const seed = if (it.next()) |a| try std.fmt.parseInt(u64, a, 10) else 42;
    const ntask = if (it.next()) |a| try std.fmt.parseInt(u32, a, 10) else 64;
    const virt_s = if (it.next()) |a| try std.fmt.parseInt(i64, a, 10) else 60;

    const w0 = wallNs();
    const a = once(0, seed, ntask, virt_s, .{});
    const wall_a: u64 = @intCast(@max(1, wallNs() - w0));
    const b = once(1, seed, ntask, virt_s, .{});
    const c = once(2, seed +% 1, ntask, virt_s, .{});
    const d = once(3, seed, ntask, virt_s, .{ .quantum = 100 });
    const e = once(4, seed, ntask, virt_s, .{ .conn_quota = 20000 });
    const f = once(5, seed, ntask, virt_s, .{ .conn_quota = 20000 });
    // Past the wheel's 4096ms revolution, so every deadline parks on the
    // overflow list instead of a slot. Nothing else here goes there: 3000ms
    // fits inside one revolution, and so did every deadline either server
    // armed, which is why a re-arm that corrupted the overflow list into a
    // ring went unnoticed until it hung a real run at 100% CPU.
    const g = once(6, seed, ntask, virt_s, .{ .idle_deadline_ms = 10_000 });
    // H and I: a quarter of connections get cancelled at some point, so the
    // whole path runs. Nothing above ever cancelled anything, which meant the
    // one mechanism this project exists to study had no coverage here at all
    // while a socket test drove it a handful of times.
    const h = once(7, seed, ntask, virt_s, .{ .cancel_permille = 250 });
    const h_again = once(8, seed, ntask, virt_s, .{ .cancel_permille = 250 });

    std.debug.print(
        \\simulated {d} tasks for {d} virtual seconds in {d:.1} ms of wall clock
        \\  ({d:.0}x faster than real time; clock jumps = {d})
        \\
        \\run                       trace hash        steps   dispatch   served  deadlines  denials
        \\
    , .{ ntask, virt_s, @as(f64, @floatFromInt(wall_a)) / 1e6, @as(f64, @floatFromInt(virt_s)) * 1e9 / @as(f64, @floatFromInt(wall_a)), a.clock.jumps });

    const rows = [_]struct { name: []const u8, sm: *Sim }{
        .{ .name = "A  seed=N, default", .sm = a },
        .{ .name = "B  seed=N, default", .sm = b },
        .{ .name = "C  seed=N+1", .sm = c },
        .{ .name = "D  seed=N, quantum=100", .sm = d },
        .{ .name = "E  seed=N, conn_quota", .sm = e },
        .{ .name = "F  seed=N, conn_quota", .sm = f },
        .{ .name = "G  seed=N, deadline=10s", .sm = g },
        .{ .name = "H  seed=N, 25% cancelled", .sm = h },
        .{ .name = "I  seed=N, 25% cancelled", .sm = h_again },
    };
    for (rows) |r| {
        std.debug.print("{s:<24} {x:0>16} {d:>8} {d:>10} {d:>8} {d:>10} {d:>8}\n", .{
            r.name, r.sm.trace, r.sm.steps, r.sm.dispatches, served(r.sm), r.sm.deadline_wakes, r.sm.budget_denials,
        });
    }

    std.debug.print("\nREPLAY      A == B          : {}\n", .{a.trace == b.trace});
    std.debug.print("REPLAY      E == F          : {}\n", .{e.trace == f.trace});
    std.debug.print("SENSITIVITY A != C (seed)   : {}\n", .{a.trace != c.trace});
    std.debug.print("SENSITIVITY A != D (quantum): {}\n", .{a.trace != d.trace});
    std.debug.print("SENSITIVITY A != E (quota)  : {}\n", .{a.trace != e.trace});
    // Cancellation, which nothing here used to touch.
    var live: u64 = 0;
    for (h.tasks[1 .. ntask + 1]) |tk| {
        if (tk.live) live += 1;
    }
    std.debug.print(
        "\nCANCEL      H: {d} admitted, {d} cancels taken, {d} stale, {d} unwound, {d} still live\n",
        .{ h.admits, h.cancels_taken, h.cancels_stale, h.unwinds, live },
    );
    std.debug.print("REPLAY      H == I          : {}\n", .{h.trace == h_again.trace});
    std.debug.print("SENSITIVITY A != H (cancel) : {}\n", .{a.trace != h.trace});
    // Everything admitted has been released or is still here. The sim recycles
    // slots, so this is a real accounting question rather than a tautology.
    std.debug.print("CONSERVE    H admits == unwinds + live: {} ({d} == {d} + {d})\n", .{
        h.admits == h.unwinds + live, h.admits, h.unwinds, live,
    });

    // What the scheduler said about itself, every 64 dispatches, in every run.
    var bad: usize = 0;
    for (rows) |row| {
        if (row.sm.violation) |v| {
            bad += 1;
            std.debug.print("INVARIANT   {s}: {f} after {d} dispatches\n", .{ row.name, v, row.sm.violated_at });
        }
    }
    std.debug.print("INVARIANT   all runs clean  : {}\n", .{bad == 0});
    if (bad != 0) std.process.exit(1);

    // G is not a sensitivity run. Its trace matches A's because no deadline
    // ever fires in this workload, so lengthening the deadline changes nothing
    // observable. It is here to reach the overflow list at all, which every
    // other run does exactly zero times.
    std.debug.print("OVERFLOW    arms past the wheel: A {d}, G {d} (G trace == A: {})\n", .{
        a.s.overflows, g.s.overflows, a.trace == g.trace,
    });
}
