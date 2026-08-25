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
//! usage: sim <seed> <tasks> <virtual_seconds> [conn_quota] [work_budget]

const std = @import("std");
const sched = @import("coopkernel").sched;
const Clock = @import("coopkernel").clock.Clock;
const quota = @import("coopkernel").quota;
const linux = std.os.linux;

fn wallNs() i64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, @intCast(ts.sec)) * 1_000_000_000 + @as(i64, @intCast(ts.nsec));
}

const Sched = sched.Sched;
const TaskId = sched.TaskId;

const sup_root: quota.Id = 0;
const sup_conn: quota.Id = 1;

/// A scripted I/O readiness event: at virtual time `at_ns`, task `task`
/// becomes runnable. Stands in for the reactor.
const Event = struct { at_ns: i64, task: TaskId };

const Phase = enum { reading, working, writing, parked };

const Task = struct {
    phase: Phase = .parked,
    work_left: i64 = 0,
    served: u64 = 0,
    ended: u64 = 0,
};

const Sim = struct {
    s: Sched = .{},
    q: quota.Tree = .{},
    clock: Clock,
    rng: std.Random.DefaultPrng,
    tasks: [512]Task = @splat(.{}),
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

    fn mix(sm: *Sim, a: u64) void {
        sm.trace ^= a;
        sm.trace *%= 0x100000001b3;
    }

    fn pushEvent(sm: *Sim, at_ns: i64, task: TaskId) void {
        if (sm.ev_len == sm.ev.len) return;
        var i = sm.ev_len;
        while (i > 0 and sm.ev[i - 1].at_ns > at_ns) : (i -= 1) sm.ev[i] = sm.ev[i - 1];
        sm.ev[i] = .{ .at_ns = at_ns, .task = task };
        sm.ev_len += 1;
    }

    fn nextEventNs(sm: *const Sim) ?i64 {
        if (sm.ev_len == 0) return null;
        return sm.ev[0].at_ns;
    }

    fn popDue(sm: *Sim, now_ns: i64) ?TaskId {
        if (sm.ev_len == 0 or sm.ev[0].at_ns > now_ns) return null;
        const t = sm.ev[0].task;
        std.mem.copyForwards(Event, sm.ev[0 .. sm.ev_len - 1], sm.ev[1..sm.ev_len]);
        sm.ev_len -= 1;
        return t;
    }

    // ---------------------------------------------------------- the model

    fn run(sm: *Sim, until_ns: i64, cfg: Cfg) void {
        const r = sm.rng.random();

        // Every task starts parked with a request arriving at a random time.
        for (1..sm.n_tasks + 1) |i| {
            const t: TaskId = @intCast(i);
            sm.s.setPrio(t, 2);
            sm.s.assignQuota(t, sup_conn);
            sm.s.admit(t, cfg.cap, cfg.reserve);
            _ = sm.s.popRunnable(); // consume the spawn wake; arrival drives it
            sm.s.queued[t] = false;
            sm.pushEvent(sm.clock.v_ns + r.intRangeAtMost(i64, 0, 50_000_000), t);
            sm.s.arm(t, sm.clock.ms() + cfg.idle_deadline_ms);
        }

        while (sm.clock.v_ns < until_ns) {
            sm.steps += 1;

            // 1. Drain runnable at the current virtual instant.
            while (sm.s.popRunnable()) |t| {
                sm.dispatches += 1;
                sm.mix(@as(u64, t) << 32 | @intFromEnum(sm.s.reasonFor(t)));
                sm.mix(@bitCast(sm.clock.v_ns));
                sm.step(t, cfg, r);
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
            while (sm.popDue(sm.clock.v_ns)) |t| sm.s.makeRunnable(t, .io);
            const nm = sm.clock.ms();
            sm.q.refillPeriodic(nm);
            const before = sm.s.fires;
            sm.s.expire(nm);
            sm.deadline_wakes += sm.s.fires - before;
        }
    }

    fn step(sm: *Sim, t: TaskId, cfg: Cfg, r: std.Random) void {
        const tk = &sm.tasks[t];

        if (sm.s.reasonFor(t) == .deadline) {
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
                    sm.s.disarm(t);
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
                sm.s.disarm(t);
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
};

/// `Sim` embeds a whole `Sched` (~0.6 MB), so six of them will not fit on an
/// 8 MB stack. Static storage; the simulation is single-threaded by design.
var sims: [6]Sim = undefined;

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
    var argv: [8][]const u8 = undefined;
    var argc: usize = 0;
    for (init.args.vector) |a| {
        if (argc == 8) break;
        argv[argc] = std.mem.span(a);
        argc += 1;
    }
    const seed = if (argc > 1) try std.fmt.parseInt(u64, argv[1], 10) else 42;
    const ntask = if (argc > 2) try std.fmt.parseInt(u32, argv[2], 10) else 64;
    const virt_s = if (argc > 3) try std.fmt.parseInt(i64, argv[3], 10) else 60;

    const w0 = wallNs();
    const a = once(0, seed, ntask, virt_s, .{});
    const wall_a: u64 = @intCast(@max(1, wallNs() - w0));
    const b = once(1, seed, ntask, virt_s, .{});
    const c = once(2, seed +% 1, ntask, virt_s, .{});
    const d = once(3, seed, ntask, virt_s, .{ .quantum = 100 });
    const e = once(4, seed, ntask, virt_s, .{ .conn_quota = 20000 });
    const f = once(5, seed, ntask, virt_s, .{ .conn_quota = 20000 });

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
}
