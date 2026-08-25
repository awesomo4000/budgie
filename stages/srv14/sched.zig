//! The scheduler. Knows about tasks, runnability, deadlines, and budgets.
//!
//! It does not know what a socket is. Nothing in this file mentions fds, poll,
//! epoll, or bytes. The reactor calls `makeRunnable` when I/O is ready; the
//! application calls `charge` when it does work. That is the entire contract.
//!
//! Two structures replace the two O(max_tasks) scans in the fused version:
//!
//!   - runnable: FIFO ring + a `queued` bit per task, so "who can run" is
//!     O(ready) instead of O(max_tasks).
//!   - deadlines: single-level timer wheel, `wheel_slots` buckets at 1ms
//!     granularity, with intrusive doubly-linked nodes stored in the task
//!     array. arm/disarm/rearm are all O(1) pointer surgery -- no stale
//!     entries exist to collect, so no slack window is needed to avoid
//!     paying for them. An occupancy bitmap lets "when is the next timer"
//!     skip empty buckets a word at a time instead of scanning.

const std = @import("std");

pub const TaskId = u32;

/// What a charge was for. Units are the enforced, deterministic currency;
/// nanoseconds are observed alongside and NEVER read by control flow, which is
/// what keeps execution a pure function of the inputs.
pub const Account = enum {
    accept,
    parse,
    work,
    write,
    cleanup,
    background,

    pub const count = @typeInfo(Account).@"enum".fields.len;
};

/// Set false to compile out every clock read on the charge path.
pub const observe_ns = true;

/// Strict priority classes, 0 = highest. Dispatch picks the highest non-empty
/// class; budget then bounds how long the chosen task may run. The two are
/// orthogonal and both must pass:
///
///   priority -> who runs      (a lower class waits while a higher one has work)
///   budget   -> how long      (a task with no budget cannot run at all)
///
/// Priority alone starves the bottom class forever; budget alone cannot say
/// "only when there is slack". Together they give SCHED_IDLE semantics without
/// leaving the scheduler -- which keeps it deterministic and replayable.
// ------------------------------------------------------------- supervisors
//
// A budget stops being a number on a task and becomes an object a task draws
// from. That is the difference between "each connection may spend 1000" and
// "all connections together may spend 1000 per period" -- the second is not
// expressible with per-task fields at all, and it is the one that actually
// bounds a class.
//
// Tasks hold a *grant*, not a budget. When the grant runs out the task asks
// its supervisor for another; the supervisor deducts from itself AND from
// every ancestor, so a child can never outspend its parent. Nothing conjures
// units: `topUp` is the only way budget enters a task, and it always comes
// out of somewhere.

pub const SupId = u8;
pub const max_sups = 16;
pub const no_sup: SupId = 255;
pub const unlimited: i64 = std.math.maxInt(i64) / 4;

pub const Supervisor = struct {
    parent: SupId = no_sup,
    /// Units restored at each refill. `unlimited` for an unmetered class.
    quota: i64 = 0,
    balance: i64 = 0,
    period_ms: i64 = 100,
    next_refill: i64 = 0,
    /// How much this supervisor has handed down, and how often it said no.
    granted: i64 = 0,
    denials: u64 = 0,
    tag: []const u8 = "",
};

pub const prio_levels = 4;
pub const prio_idle: u8 = prio_levels - 1;
pub const max_tasks = 8192;

/// Why a task became runnable. The app switches on this instead of scanning
/// for its own timeout condition.
pub const Wake = enum { spawn, io, deadline, cancelled };

/// A minted right to cancel one specific task instance. Generational: a token
/// for a task whose slot has since been recycled is a no-op, not a kill of
/// whoever inherited the slot. Same discipline as a pool handle.
pub const CancelTok = struct { task: TaskId, gen: u32 };

pub const wheel_slots: usize = 4096; // 1ms per slot => 4.096s of range
const nil: u32 = std.math.maxInt(u32);

/// Intrusive timer node. Lives in the task array, so unlinking is O(1) and
/// needs no lookup.
const Timer = struct {
    prev: u32 = nil,
    next: u32 = nil,
    slot: u32 = nil, // nil = not armed
    at: i64 = 0,
};

pub const Sched = struct {
    live: [max_tasks]bool = @splat(false),

    // --- runnable rings, one per priority class ---
    ring: [prio_levels][max_tasks]TaskId = undefined,
    head: [prio_levels]usize = @splat(0),
    len: [prio_levels]usize = @splat(0),
    /// Bit c set means class c has runnable work. @ctz finds the highest
    /// priority non-empty class in one instruction.
    nonempty: u32 = 0,
    prio: [max_tasks]u8 = @splat(1),
    queued: [max_tasks]bool = @splat(false),
    wake: [max_tasks]Wake = @splat(.spawn),
    ran_by_class: [prio_levels]u64 = @splat(0),

    // --- budgets (caller-pays; reserve is untouchable by the body) ---
    /// Grant currently held. Refilled from the supervisor by `topUp`.
    budget: [max_tasks]i64 = @splat(0),
    /// Per-request ceiling. The supervisor bounds the CLASS; this bounds one
    /// request. Without it a task with an unlimited supervisor tops up
    /// forever and a single request can spend the whole class allowance --
    /// which is exactly what the supervisor refactor broke.
    cap: [max_tasks]i64 = @splat(0),
    reserve: [max_tasks]i64 = @splat(0),

    // --- deadlines: timer wheel ---
    timer: [max_tasks]Timer = @splat(.{}),
    bucket: [wheel_slots]u32 = @splat(nil),
    occupied: [wheel_slots / 64]u64 = @splat(0),
    cur: i64 = 0, // wheel has fired everything strictly before this ms
    armed: usize = 0,
    overflow: u32 = nil, // deadlines beyond one revolution

    // --- cancellation ---
    gen: [max_tasks]u32 = @splat(0),
    cancelled: [max_tasks]bool = @splat(false),

    // --- two-currency accounting ---
    /// Enforced currency. Deterministic: same input, same count, every run.
    units_by: [Account.count]i64 = @splat(0),
    /// Observed currency. Recorded, reported, and never branched on.
    ns_by: [Account.count]i64 = @splat(0),
    calls_by: [Account.count]u64 = @splat(0),
    denied_by: [Account.count]u64 = @splat(0),

    grant_size: i64 = 1000,

    // --- supervisors ---
    sups: [max_sups]Supervisor = @splat(.{}),
    /// Default is the ROOT supervisor, not `no_sup`. An admission mechanism
    /// whose default is "unmetered" grants infinite budget to anything the
    /// caller forgot to assign -- which is exactly the task you least want
    /// unmetered. Fail closed.
    sup_of: [max_tasks]SupId = @splat(0),
    top_ups: u64 = 0,
    top_up_denials: u64 = 0,

    // stats
    fires: u64 = 0,
    rearms: u64 = 0,
    cancels: u64 = 0,
    cancels_stale: u64 = 0,

    // ------------------------------------------------------------ lifecycle

    /// `cap` is the PER-REQUEST ceiling, not a pre-funded grant: `budget`
    /// starts at zero and units only ever arrive through `topUp`, so the
    /// supervisor tree stays conservative.
    pub fn admit(s: *Sched, id: TaskId, cap: i64, reserve: i64) void {
        s.live[id] = true;
        s.cap[id] = cap;
        s.cancelled[id] = false;
        s.gen[id] +%= 1;
        s.budget[id] = 0;
        s.reserve[id] = reserve;
        s.makeRunnable(id, .spawn);
    }

    pub fn release(s: *Sched, id: TaskId) void {
        s.disarm(id);
        s.live[id] = false;
        s.cancelled[id] = false;
        s.gen[id] +%= 1; // invalidates every outstanding token for this slot
    }

    // ---------------------------------------------------------- cancellation

    pub fn cancelTok(s: *const Sched, id: TaskId) CancelTok {
        return .{ .task = id, .gen = s.gen[id] };
    }

    /// Cancel is two independent guarantees:
    ///   safety   - budget = 0, so the task structurally cannot do more body
    ///              work even if the app's handling is buggy. The reserve is
    ///              untouched, so the unwind stays funded.
    ///   liveness - makeRunnable, so a task parked in epoll notices now
    ///              rather than at its deadline.
    /// Sticky, idempotent, and not a value anything can swallow.
    pub fn cancel(s: *Sched, tok: CancelTok) bool {
        if (!s.live[tok.task] or s.gen[tok.task] != tok.gen) {
            s.cancels_stale += 1;
            return false;
        }
        if (s.cancelled[tok.task]) return true;
        s.cancelled[tok.task] = true;
        s.cancels += 1;
        s.budget[tok.task] = 0;
        s.cap[tok.task] = 0; // and no further top-ups can be requested
        s.disarm(tok.task);
        s.makeRunnable(tok.task, .cancelled);
        return true;
    }

    pub fn isCancelled(s: *const Sched, id: TaskId) bool {
        return s.cancelled[id];
    }

    /// Refresh the body budget without touching the reserve.
    /// Reserve is a separate grant that is always honoured -- an unwind must
    /// never be blocked by an exhausted supervisor, or cleanup becomes the
    /// thing that fails under load.
    pub fn setReserve(s: *Sched, id: TaskId, reserve: i64) void {
        if (!s.cancelled[id]) s.reserve[id] = reserve;
    }

    /// Start of a new request on an existing task: restore the per-request
    /// ceiling. Does NOT hand out units -- those still come from the
    /// supervisor via `topUp`, so conservation is preserved.
    pub fn renewCap(s: *Sched, id: TaskId, cap: i64) void {
        if (!s.cancelled[id]) s.cap[id] = cap;
    }

    // ------------------------------------------------------- supervisors

    pub fn defineSup(s: *Sched, id: SupId, parent: SupId, quota: i64, period_ms: i64, tag: []const u8) void {
        s.sups[id] = .{
            .parent = parent,
            .quota = quota,
            .balance = quota,
            .period_ms = period_ms,
            .next_refill = 0,
            .tag = tag,
        };
    }

    pub fn assignSup(s: *Sched, id: TaskId, sup: SupId) void {
        s.sup_of[id] = sup;
    }

    /// Draw `want` units into the task's grant, deducting from its supervisor
    /// and every ancestor. All-or-nothing: a partial walk is rolled back, so
    /// a denial never silently spends a parent's balance.
    pub fn topUp(s: *Sched, id: TaskId, want: i64) bool {
        if (s.cancelled[id]) return false;
        if (want <= 0) return true;
        var sup = s.sup_of[id];
        if (sup == no_sup) { // explicitly unsupervised is a denial, not a blank cheque
            s.top_up_denials += 1;
            return false;
        }
        var chain: [8]SupId = undefined;
        var n: usize = 0;
        while (sup != no_sup and n < chain.len) : (n += 1) {
            if (s.sups[sup].balance < want) {
                s.sups[sup].denials += 1;
                s.top_up_denials += 1;
                return false; // nothing deducted yet at this level or above
            }
            chain[n] = sup;
            sup = s.sups[sup].parent;
        }
        for (chain[0..n]) |c| {
            s.sups[c].balance -= want;
            s.sups[c].granted += want;
        }
        s.budget[id] += want;
        s.top_ups += 1;
        return true;
    }

    /// Restore supervisor balances whose period has elapsed. Called once per
    /// kernel step; O(max_sups), which is 16.
    pub fn refillSups(s: *Sched, now: i64) void {
        for (&s.sups) |*sp| {
            if (sp.quota == 0) continue;
            if (sp.next_refill == 0) sp.next_refill = now + sp.period_ms;
            if (now >= sp.next_refill) {
                sp.balance = sp.quota;
                sp.next_refill = now + sp.period_ms;
            }
        }
    }

    /// The soonest supervisor refill, so the reactor does not oversleep past
    /// the moment a throttled class becomes runnable again.
    pub fn nextRefillMs(s: *const Sched, now: i64) ?i64 {
        var best: ?i64 = null;
        for (s.sups) |sp| {
            if (sp.quota == 0 or sp.quota >= unlimited) continue;
            if (sp.balance > 0) continue; // not blocked, no need to wake for it
            const at = sp.next_refill;
            if (at == 0) continue;
            if (best == null or at < best.?) best = at;
        }
        if (best) |bb| return @max(0, bb - now);
        return null;
    }

    // ------------------------------------------------------------- runnable

    pub fn setPrio(s: *Sched, id: TaskId, p: u8) void {
        s.prio[id] = @min(p, prio_idle);
    }

    pub fn makeRunnable(s: *Sched, id: TaskId, why: Wake) void {
        if (!s.live[id] or s.queued[id]) return;
        s.queued[id] = true;
        s.wake[id] = why;
        const c = s.prio[id];
        s.ring[c][(s.head[c] + s.len[c]) % max_tasks] = id;
        s.len[c] += 1;
        s.nonempty |= @as(u32, 1) << @intCast(c);
    }

    /// Highest non-empty class wins, FIFO within the class.
    pub fn popRunnable(s: *Sched) ?TaskId {
        while (s.nonempty != 0) {
            const c: usize = @ctz(s.nonempty);
            while (s.len[c] > 0) {
                const id = s.ring[c][s.head[c]];
                s.head[c] = (s.head[c] + 1) % max_tasks;
                s.len[c] -= 1;
                if (s.len[c] == 0) s.nonempty &= ~(@as(u32, 1) << @intCast(c));
                s.queued[id] = false;
                if (s.live[id]) {
                    s.ran_by_class[c] += 1;
                    return id;
                }
            }
            s.nonempty &= ~(@as(u32, 1) << @intCast(c));
        }
        return null;
    }

    /// True when work exists in any class strictly above `c`.
    pub fn runnableAbove(s: *const Sched, c: u8) bool {
        const mask = (@as(u32, 1) << @intCast(c)) - 1;
        return (s.nonempty & mask) != 0;
    }

    pub fn reasonFor(s: *const Sched, id: TaskId) Wake {
        return s.wake[id];
    }

    pub fn anyRunnable(s: *const Sched) bool {
        return s.nonempty != 0;
    }

    // ------------------------------------------------------- deadlines

    /// O(1). Rearming an already-armed task unlinks and relinks; there is no
    /// stale entry left behind, which is why no slack window is needed.
    pub fn arm(s: *Sched, id: TaskId, at_ms: i64) void {
        if (s.timer[id].slot != nil) {
            s.rearms += 1;
            s.unlink(id);
        }
        if (s.cur == 0) s.cur = at_ms;
        const t = &s.timer[id];
        t.at = at_ms;
        const delta = at_ms - s.cur;
        if (delta >= @as(i64, wheel_slots)) {
            // Beyond one revolution: park on the overflow list, swept back in
            // when the wheel catches up.
            t.slot = nil;
            t.prev = nil;
            t.next = s.overflow;
            if (s.overflow != nil) s.timer[s.overflow].prev = id;
            s.overflow = id;
            s.armed += 1;
            return;
        }
        const slot: u32 = @intCast(@as(usize, @intCast(@max(0, at_ms))) % wheel_slots);
        s.link(id, slot);
        s.armed += 1;
    }

    pub fn disarm(s: *Sched, id: TaskId) void {
        const t = &s.timer[id];
        if (t.slot == nil and t.prev == nil and t.next == nil and s.overflow != id) return;
        s.unlink(id);
    }

    fn link(s: *Sched, id: TaskId, slot: u32) void {
        const t = &s.timer[id];
        t.slot = slot;
        t.prev = nil;
        t.next = s.bucket[slot];
        if (t.next != nil) s.timer[t.next].prev = id;
        s.bucket[slot] = id;
        s.occupied[slot / 64] |= @as(u64, 1) << @intCast(slot % 64);
    }

    fn unlink(s: *Sched, id: TaskId) void {
        const t = &s.timer[id];
        const in_overflow = t.slot == nil;
        if (t.prev != nil) {
            s.timer[t.prev].next = t.next;
        } else if (in_overflow) {
            if (s.overflow == id) s.overflow = t.next;
        } else {
            s.bucket[t.slot] = t.next;
            if (t.next == nil) s.occupied[t.slot / 64] &= ~(@as(u64, 1) << @intCast(t.slot % 64));
        }
        if (t.next != nil) s.timer[t.next].prev = t.prev;
        t.prev = nil;
        t.next = nil;
        t.slot = nil;
        if (s.armed > 0) s.armed -= 1;
    }

    /// Absolute ms of the next occupied slot at or after `from`, within one
    /// revolution. Skips empty buckets 64 at a time via the bitmap.
    fn nextOccupied(s: *const Sched, from: i64) ?i64 {
        if (s.armed == 0) return null;
        const base: usize = @intCast(@max(0, from));
        var k: usize = 0;
        while (k < wheel_slots) {
            const slot = (base + k) % wheel_slots;
            const word = s.occupied[slot / 64];
            const bit: u6 = @intCast(slot % 64);
            const masked = word >> bit;
            if (masked != 0) {
                const adv = @ctz(masked);
                return from + @as(i64, @intCast(k + adv));
            }
            k += 64 - @as(usize, bit);
        }
        return null;
    }

    /// Milliseconds the caller may block, or null for "nothing armed".
    pub fn timeoutMs(s: *Sched, now: i64) ?i64 {
        if (s.armed == 0) return null;
        var best: ?i64 = s.nextOccupied(@max(s.cur, now));
        // Overflow is expected empty; scanning it is O(overflow), and it must
        // not mask a nearer in-wheel timer.
        var id = s.overflow;
        while (id != nil) : (id = s.timer[id].next) {
            const at = s.timer[id].at;
            if (best == null or at < best.?) best = at;
        }
        const b = best orelse return null;
        return @max(0, b - now);
    }

    /// Fire everything due at or before `now`. O(fired + occupied words).
    pub fn expire(s: *Sched, now: i64) void {
        if (s.cur == 0) s.cur = now;
        // Sweep first: an overflow entry that has come due must fire in this
        // call, not the next one.
        s.sweepOverflow(now);
        while (s.cur <= now) {
            const at = s.nextOccupied(s.cur) orelse break;
            if (at > now) break;
            const slot: u32 = @intCast(@as(usize, @intCast(at)) % wheel_slots);
            while (s.bucket[slot] != nil) {
                const id = s.bucket[slot];
                s.unlink(id);
                s.fires += 1;
                s.makeRunnable(id, .deadline);
            }
            s.cur = at + 1;
        }
        s.cur = @max(s.cur, now);
    }

    fn sweepOverflow(s: *Sched, now: i64) void {
        var id = s.overflow;
        while (id != nil) {
            const nxt = s.timer[id].next;
            const at = s.timer[id].at;
            if (at <= now) {
                // Already due. Fire directly rather than linking into a slot
                // the wheel hand has passed, which would wrap into the future.
                s.unlink(id);
                s.fires += 1;
                s.makeRunnable(id, .deadline);
            } else if (at - s.cur < @as(i64, wheel_slots)) {
                s.unlink(id);
                s.armed += 1;
                s.link(id, @intCast(@as(usize, @intCast(at)) % wheel_slots));
            }
            id = nxt;
        }
    }

    // --------------------------------------------------------------- budgets

    /// Spend from the body budget. Returns false if it will not fit — the
    /// caller transitions to cleanup rather than being killed.
    pub fn charge(s: *Sched, id: TaskId, units: i64) bool {
        if (units > s.budget[id]) return false;
        s.budget[id] -= units;
        return true;
    }

    /// Charge the enforced currency and attribute it to an account.
    pub fn chargeTo(s: *Sched, id: TaskId, a: Account, units: i64) bool {
        const i = @intFromEnum(a);
        s.calls_by[i] += 1;
        // Per-request ceiling first: a supervisor with room does not entitle
        // one request to spend without limit.
        if (units > s.cap[id]) {
            s.denied_by[i] += 1;
            return false;
        }
        if (units > s.budget[id]) {
            // Grant exhausted: ask the supervisor for another, never for more
            // than this request is still allowed to spend.
            const want = @min(@max(units, s.grant_size), s.cap[id]);
            if (!s.topUp(id, want)) {
                s.denied_by[i] += 1;
                return false;
            }
        }
        s.budget[id] -= units;
        s.cap[id] -= units;
        s.units_by[i] += units;
        return true;
    }

    /// Record observed nanoseconds against an account.
    ///
    /// Nothing in this file, or anywhere else, reads `ns_by` to make a
    /// decision. Enforcement is units-only, so budget exhaustion lands at the
    /// identical point on every replay of the same input. The ns currency
    /// exists to calibrate units to latency, to detect drift in ns-per-unit,
    /// and -- the important one -- to expose wall time that charges no units
    /// at all, which is where the amortised costs a fault can never catch
    /// actually live.
    pub fn observe(s: *Sched, a: Account, ns: i64) void {
        if (!observe_ns) return;
        s.ns_by[@intFromEnum(a)] += ns;
    }

    pub fn observedNsTotal(s: *const Sched) i64 {
        var t: i64 = 0;
        for (s.ns_by) |v| t += v;
        return t;
    }

    /// Spend from the reserve. Only the unwind path calls this, which is the
    /// entire enforcement: no other function in the program touches it.
    pub fn chargeReserve(s: *Sched, id: TaskId, units: i64) void {
        s.reserve[id] -= units;
    }
};
