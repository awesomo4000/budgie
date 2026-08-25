//! A teeny-tiny cooperative kernel in userspace.
//!
//! Everything here is single-threaded. The only asynchrony is a SIGALRM
//! delivered by ITIMER_REAL. The point of the exercise is the shape of the
//! signal path, not the scheduler:
//!
//!   - `in_kernel` is set while the kernel owns the world and cleared while a
//!     task runs. The handler reads it and does nothing else with it except
//!     bookkeeping — but that bookkeeping is exactly the measurement that
//!     tells you whether a real (preempting) kernel would have been allowed to
//!     switch stacks at that instant.
//!
//!   - The handler NEVER touches kernel structures. It bumps two atomics and
//!     stashes a timestamp. Everything else is deferred to the next kernel
//!     entry, which happens between task quanta. Nothing in this file needs to
//!     be async-signal-safe except `onTick`.
//!
//!   - Because the kernel is cooperative, the delay between "signal arrived"
//!     and "kernel noticed" is bounded by the length of one task quantum, and
//!     nothing else. That number is measured and reported: it is the entire
//!     argument for why quantum size matters.
//!
//! The workload: task A advances a u128 counter, task B trial-divides the
//! numbers A produced. B gets slower as sqrt(n), so the counter's rate decays
//! visibly across timer ticks.

const std = @import("std");
const linux = std.os.linux;
const posix = std.posix;

// ---------------------------------------------------------------- timer glue

/// The `setitimer` syscall takes microseconds, not nanoseconds, so it wants
/// `itimerval` rather than the `itimerspec` that std's binding hands it.
/// Declared here so the units are unambiguous.
const timeval = extern struct { sec: isize, usec: isize };
const itimerval = extern struct { it_interval: timeval, it_value: timeval };
const ITIMER_REAL: usize = 0;

fn armTimer(ms: u64) !void {
    const v: itimerval = .{
        .it_interval = .{ .sec = @intCast(ms / 1000), .usec = @intCast((ms % 1000) * 1000) },
        .it_value = .{ .sec = @intCast(ms / 1000), .usec = @intCast((ms % 1000) * 1000) },
    };
    if (linux.syscall3(.setitimer, ITIMER_REAL, @intFromPtr(&v), 0) != 0)
        return error.SetItimerFailed;
}

fn nowNs() i64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, @intCast(ts.sec)) * 1_000_000_000 + @as(i64, @intCast(ts.nsec));
}

// ------------------------------------------------------------- kernel state

/// Fields the signal handler may touch. Nothing else is reachable from it.
const Shared = struct {
    /// True while the kernel owns the world. A preempting kernel would consult
    /// this to decide whether it is allowed to switch stacks right now.
    in_kernel: std.atomic.Value(bool) = .init(true),
    /// Set by the handler, drained by the kernel at its next entry.
    pending: std.atomic.Value(u32) = .init(0),
    /// Monotonic ns at which the OLDEST undrained signal arrived.
    ///
    /// Deliberately i64, not i128. `std.atomic.Value(i128).store` segfaults in
    /// Debug on x86_64-linux with zig 0.16.0 — Debug lowers it to a
    /// `lock cmpxchg16b` reached through a null double-indirection and faults
    /// at 0x8. ReleaseFast lowers the same store to one `vmovdqa` and works;
    /// `-mcpu=baseline` refuses to compile it at all. Repro is six lines and
    /// involves no signals.
    ///
    /// Independently: on targets without cmpxchg16b, a 128-bit atomic lowers
    /// to `__atomic_store_16`, which takes a lock from compiler-rt's spinlock
    /// table and is therefore not async-signal-safe. Either reason alone
    /// rules i128 out of this struct. Monotonic ns in an i64 covers ~292
    /// years of uptime.
    arrived_ns: std.atomic.Value(i64) = .init(0),
    took_in_kernel: std.atomic.Value(u64) = .init(0),
    took_in_task: std.atomic.Value(u64) = .init(0),
};

var shared: Shared = .{};

/// The whole async-signal-safe surface of this program.
fn onTick(_: posix.SIG) callconv(.c) void {
    if (shared.in_kernel.load(.monotonic)) {
        _ = shared.took_in_kernel.fetchAdd(1, .monotonic);
    } else {
        _ = shared.took_in_task.fetchAdd(1, .monotonic);
    }
    // Record the arrival of the FIRST undrained signal only. Later ones
    // coalesce onto it, so `arrived_ns` measures how long the oldest pending
    // tick has been waiting for the kernel — the number that actually bounds
    // scheduling latency.
    if (shared.pending.fetchAdd(1, .acq_rel) == 0) {
        shared.arrived_ns.store(nowNs(), .monotonic);
    }
}

const Task = struct {
    name: []const u8,
    run: *const fn (*Kernel) void,
    quanta: u64 = 0,
};

const Kernel = struct {
    tasks: [2]Task,
    current: usize = 0,
    steps: u64 = 0,

    // config
    batch: u64,
    print_every: u64,
    time_quanta: bool = false,

    // workload state, owned by the kernel, shared between tasks
    n: u128,
    checked: u128,
    primes: u64 = 0,

    // tick accounting
    ticks: u64 = 0,
    started_ns: i64 = 0,
    last_n: u128 = 0,
    last_primes: u64 = 0,
    last_steps: u64 = 0,
    last_tick_ns: i64 = 0,
    defer_last_ns: i64 = 0,
    defer_max_ns: i64 = 0,
    coalesced: u64 = 0,

    const StepInfo = struct {
        index: u64,
        ran: []const u8,
        ticked: bool,
        deferral_ns: i64,
        quantum_ns: i64,
        /// How many timer signals collapsed into this one delivery. Anything
        /// above 1 is a tick the kernel never saw as a separate event; a real
        /// budget scheme has to charge all of them, not one.
        drained: u32,
    };

    /// One scheduler iteration. This is the entire kernel.
    fn step(k: *Kernel) StepInfo {
        k.steps += 1;
        // We are in the kernel for the whole of this function except the call
        // into the task.
        var info: StepInfo = .{
            .index = k.steps,
            .ran = "",
            .ticked = false,
            .deferral_ns = 0,
            .quantum_ns = 0,
            .drained = 0,
        };

        // 1. Drain the pending signal. This is the only place timer work runs.
        // Read the timestamp before clearing the flag: if a signal lands in
        // the window between the two, we over-report latency rather than
        // under-report it.
        const arrived = shared.arrived_ns.load(.monotonic);
        const drained = shared.pending.swap(0, .acquire);
        if (drained != 0) {
            info.deferral_ns = nowNs() - arrived;
            info.drained = drained;
            info.ticked = true;
            k.defer_last_ns = info.deferral_ns;
            if (info.deferral_ns > k.defer_max_ns) k.defer_max_ns = info.deferral_ns;
            k.onTimer(drained);
        }

        // 2. Pick a task. Round robin; a real one would consult a budget.
        const idx = k.current;
        k.current = (k.current + 1) % k.tasks.len;
        k.tasks[idx].quanta += 1;
        info.ran = k.tasks[idx].name;

        // 3. Leave the kernel, run the quantum, re-enter.
        const t0 = if (k.time_quanta) nowNs() else 0;
        shared.in_kernel.store(false, .release);
        k.tasks[idx].run(k);
        shared.in_kernel.store(true, .release);
        if (k.time_quanta) info.quantum_ns = nowNs() - t0;

        return info;
    }

    fn onTimer(k: *Kernel, drained: u32) void {
        k.ticks += 1;
        k.coalesced += drained;
        const now = nowNs();
        const window_ns = now - k.last_tick_ns;
        k.last_tick_ns = now;

        const dn = k.n - k.last_n;
        const dp = k.primes - k.last_primes;
        const ds = k.steps - k.last_steps;
        k.last_n = k.n;
        k.last_primes = k.primes;
        k.last_steps = k.steps;

        const secs = @as(f64, @floatFromInt(window_ns)) / 1e9;
        const rate = @as(f64, @floatFromInt(@as(u64, @intCast(dn)))) / secs;

        var b1: [64]u8 = undefined;
        var b2: [64]u8 = undefined;
        var b3: [64]u8 = undefined;
        var b4: [64]u8 = undefined;
        std.debug.print(
            \\
            \\[tick {d}] t={d:.2}s  n={s}  dn={s}  rate={d:.0}/s
            \\          primes={s} (+{s})   steps={d}
            \\          signals: in_kernel={d} in_task={d}  coalesced={d} (this drain: {d})
            \\          deferral: last={d:.1}us max={d:.1}us
            \\
        , .{
            k.ticks,
            @as(f64, @floatFromInt(now - k.started_ns)) / 1e9,
            commas(&b1, k.n),
            commas(&b2, dn),
            rate,
            commas(&b3, k.primes),
            commas(&b4, dp),
            ds,
            shared.took_in_kernel.load(.monotonic),
            shared.took_in_task.load(.monotonic),
            k.coalesced,
            drained,
            @as(f64, @floatFromInt(k.defer_last_ns)) / 1000.0,
            @as(f64, @floatFromInt(k.defer_max_ns)) / 1000.0,
        });
    }
};

// ------------------------------------------------------------------- tasks

/// Task A: advance the counter. Cheap, constant time.
fn taskCount(k: *Kernel) void {
    k.n += k.batch;
}

/// Task B: trial-divide everything task A produced since last time, up to one
/// batch worth. Cost grows as sqrt(n), which is the whole point.
fn taskPrime(k: *Kernel) void {
    var done: u64 = 0;
    while (k.checked < k.n and done < k.batch) : (done += 1) {
        k.checked += 1;
        if (isPrime(k.checked)) {
            k.primes += 1;
            if (k.primes % k.print_every == 0) {
                var b: [64]u8 = undefined;
                std.debug.print("  prime #{d} = {s}\n", .{ k.primes, commas(&b, k.checked) });
            }
        }
    }
}

fn isPrime(n: u128) bool {
    if (n < 2) return false;
    if (n % 2 == 0) return n == 2;
    if (n % 3 == 0) return n == 3;
    var d: u128 = 5;
    while (d * d <= n) : (d += 6) {
        if (n % d == 0) return false;
        if (n % (d + 2) == 0) return false;
    }
    return true;
}

// ------------------------------------------------------------------ helpers

fn commas(buf: *[64]u8, v: u128) []const u8 {
    var tmp: [48]u8 = undefined;
    var i: usize = tmp.len;
    var x = v;
    if (x == 0) {
        i -= 1;
        tmp[i] = '0';
    }
    var digits: usize = 0;
    while (x > 0) {
        i -= 1;
        tmp[i] = @intCast('0' + @as(u8, @intCast(x % 10)));
        x /= 10;
        digits += 1;
        if (digits % 3 == 0 and x > 0) {
            i -= 1;
            tmp[i] = ',';
        }
    }
    const src = tmp[i..];
    @memcpy(buf[0..src.len], src);
    return buf[0..src.len];
}

// --------------------------------------------------------------------- main

const usage =
    \\usage:
    \\  tick run   [start_n] [batch] [tick_ms] [ticks]
    \\  tick trace [steps]   [batch] [tick_ms]
    \\
;

pub fn main(init: std.process.Init.Minimal) !void {
    var args = std.mem.zeroes([8][]const u8);
    var argc: usize = 0;
    for (init.args.vector) |a| {
        if (argc == args.len) break;
        args[argc] = std.mem.span(a);
        argc += 1;
    }
    const mode = if (argc > 1) args[1] else "run";

    const argn = struct {
        fn get(a: [8][]const u8, n: usize, c: usize, dflt: u128) u128 {
            if (n >= c) return dflt;
            return std.fmt.parseInt(u128, a[n], 10) catch dflt;
        }
    }.get;

    var act: posix.Sigaction = .{
        .handler = .{ .handler = onTick },
        .mask = posix.sigemptyset(),
        .flags = posix.SA.RESTART,
    };
    posix.sigaction(.ALRM, &act, null);

    if (std.mem.eql(u8, mode, "trace")) {
        const steps: u64 = @intCast(argn(args, 2, argc, 24));
        const batch: u64 = @intCast(argn(args, 3, argc, 1));
        const tick_ms: u64 = @intCast(argn(args, 4, argc, 5));

        var k: Kernel = .{
            .tasks = .{
                .{ .name = "count", .run = taskCount },
                .{ .name = "prime", .run = taskPrime },
            },
            .batch = batch,
            .print_every = std.math.maxInt(u64),
            .time_quanta = true,
            .n = 1,
            .checked = 1,
        };
        k.started_ns = nowNs();
        k.last_tick_ns = k.started_ns;

        std.debug.print("step  ran      quantum    tick   deferral  drained   sigK sigT        n\n", .{});
        std.debug.print("----  -----  ---------  ------  ---------  -------   ---- ----  -------\n", .{});
        var i: u64 = 0;
        while (i < steps) : (i += 1) {
            const info = k.step();
            var b: [64]u8 = undefined;
            var qb: [24]u8 = undefined;
            var db: [24]u8 = undefined;
            std.debug.print("{d:>4}  {s:<5}  {s:>9}  {s:<6}  {s:>9}  {d:>7}   {d:>4} {d:>4}  {s:>7}\n", .{
                info.index,
                info.ran,
                std.fmt.bufPrint(&qb, "{d:.1}us", .{@as(f64, @floatFromInt(info.quantum_ns)) / 1000.0}) catch "?",
                if (info.ticked) "TICK" else "-",
                if (info.ticked)
                    std.fmt.bufPrint(&db, "{d:.1}us", .{@as(f64, @floatFromInt(info.deferral_ns)) / 1000.0}) catch "?"
                else
                    "-",
                info.drained,
                shared.took_in_kernel.load(.monotonic),
                shared.took_in_task.load(.monotonic),
                commas(&b, k.n),
            });
            // Arm the timer only after the first few steps so the trace shows
            // both the quiet path and the tick path.
            if (i == 3) try armTimer(tick_ms);
        }
        std.debug.print(
            "\nquanta: count={d} prime={d}   signals: in_kernel={d} in_task={d}\n",
            .{ k.tasks[0].quanta, k.tasks[1].quanta, shared.took_in_kernel.load(.monotonic), shared.took_in_task.load(.monotonic) },
        );
        return;
    }

    const start_n = argn(args, 2, argc, 1_000_000);
    const batch: u64 = @intCast(argn(args, 3, argc, 1));
    const tick_ms: u64 = @intCast(argn(args, 4, argc, 4000));
    const max_ticks: u64 = @intCast(argn(args, 5, argc, 7));

    var k: Kernel = .{
        .tasks = .{
            .{ .name = "count", .run = taskCount },
            .{ .name = "prime", .run = taskPrime },
        },
        .batch = batch,
        .print_every = 200_000,
        .n = start_n,
        .checked = start_n,
    };
    k.last_n = start_n;
    k.started_ns = nowNs();
    k.last_tick_ns = k.started_ns;

    var b: [64]u8 = undefined;
    std.debug.print(
        "cooperative kernel: start_n={s} batch={d} tick={d}ms ticks={d}\n",
        .{ commas(&b, start_n), batch, tick_ms, max_ticks },
    );

    try armTimer(tick_ms);
    while (k.ticks < max_ticks) _ = k.step();

    std.debug.print(
        "\nquanta: count={d} prime={d}\n",
        .{ k.tasks[0].quanta, k.tasks[1].quanta },
    );
}
