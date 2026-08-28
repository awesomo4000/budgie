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
const quota = @import("quota.zig");

pub const TaskId = u32;


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

pub const prio_levels = 4;
pub const prio_idle: u8 = prio_levels - 1;
pub const max_tasks = 8192;

/// Why a task became runnable. The app switches on this instead of scanning
/// for its own timeout condition.
pub const Wake = enum { spawn, io, deadline, cancelled };

/// What has been withdrawn from a task, if anything. See `Sched.faultOf`.
///
/// One variant, and that is the honest count rather than a placeholder. Two
/// other conditions look like they belong here and do not.
///
/// A fired deadline is not a fault. The same wheel means "you are late" for a
/// connection and "refill my budget" for a periodic task, and only the
/// application knows which of those it armed. `app/server.zig` uses it both
/// ways in the same file.
///
/// A refused `charge` is not a fault either. It fails for two unrelated
/// reasons, this request spending its per-request ceiling or the whole class
/// running out of allowance for the period, and those want different answers:
/// refuse the request, or park until the refill.
///
/// Cancellation is the one where the fact carries no policy. Somebody holding
/// the right to stop this task has stopped it. What to do about that is the
/// supervisor's business. That it happened is the scheduler's, and it is the
/// only thing here the scheduler can state without guessing at intent.
pub const Fault = enum { cancelled };

/// A minted right to cancel one specific task instance. Generational: a token
/// for a task whose slot has since been recycled is a no-op, not a kill of
/// whoever inherited the slot. Same discipline as a pool handle.
pub const CancelTok = struct {
    task: TaskId = 0,
    /// Zero is never a live generation. `admit` pre-increments and skips zero
    /// on wrap, and `cancel` refuses a token carrying it.
    ///
    /// That invariant is load bearing rather than tidy. Without it a
    /// default-constructed `.{}` is a token for task 0 at generation 0, and
    /// both servers set `live[listener_task] = true` by hand without going
    /// through admission, so the listener sits at generation 0 forever. An
    /// uninitialised token would cancel the thing that accepts connections.
    gen: u32 = 0,

    /// A token that names nothing. Use it for a field that has to exist
    /// before the task it will refer to does.
    pub const none: CancelTok = .{};
};

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


    grant_size: i64 = 1000,

    // --- quota handle, one per task ---
    //
    // The scheduler does NOT own the conservation tree. It holds an opaque
    // handle and asks. Which resource the tree meters, how it refills, and
    // what a unit means are all outside this file.
    quota_of: [max_tasks]quota.Id = @splat(0),
    top_ups: u64 = 0,
    top_up_denials: u64 = 0,

    // stats
    fires: u64 = 0,
    rearms: u64 = 0,
    /// Arms that landed past one revolution of the wheel and went on the
    /// overflow list. Counted because for a long time the answer was zero on
    /// every workload anyone ran, which is how a bug there stayed hidden.
    overflows: u64 = 0,
    cancels: u64 = 0,
    cancels_stale: u64 = 0,

    // ------------------------------------------------------------ lifecycle

    /// Everything a task needs decided before it can be dispatched. No field
    /// has a default, so the compiler makes the caller choose all four.
    ///
    /// It is one struct because the three settings used to be three separate
    /// calls that all had to happen BEFORE `admit`, and getting any of them
    /// out of order failed silently:
    ///
    ///   - `setPrio` after admission left the task queued in the old class
    ///     for its first dispatch, because `makeRunnable` reads `prio` when it
    ///     enqueues.
    ///   - `assignQuota` after admission, or not at all, charged whichever
    ///     supervisor node the slot's previous occupant used. Admission does
    ///     not reset `quota_of`, so a recycled slot inherits it.
    ///   - `cancelTok` before admission produced a token for the generation
    ///     that admission was about to bump, so the cancel silently no-opped.
    ///
    /// None of those was reachable through the type system. All three are now
    /// unreachable through it.
    pub const Admission = struct {
        prio: u8,
        quota: quota.Id,
        /// The PER-REQUEST ceiling, not a pre-funded grant: `budget` starts at
        /// zero and units only ever arrive through `topUp`, so the supervisor
        /// tree stays conservative.
        cap: i64,
        /// Funds the unwind. `charge` has no path into it and `chargeReserve`
        /// cannot fail, which is what makes cleanup possible after a cancel
        /// has taken the body budget away.
        reserve: i64,
    };

    /// Admit a task and return the sole right to cancel it.
    ///
    /// The token comes back from here because this is the only place it can
    /// honestly be minted. There was a `cancelTok(id)` that would hand anybody
    /// holding a task id the authority to cancel that task, which is ambient
    /// authority with a generation check bolted on rather than a capability.
    /// In seL4 you cannot look up a capability by object id; you receive one
    /// when the object is created, and dropping your last copy loses the
    /// authority. This is that rule, as far as a plain copyable struct can
    /// carry it: nothing here stops a holder copying the token, so it is a
    /// bearer token rather than a slot in a CNode.
    pub fn admit(s: *Sched, id: TaskId, a: Admission) CancelTok {
        s.live[id] = true;
        s.prio[id] = @min(a.prio, prio_idle);
        s.quota_of[id] = a.quota;
        s.cap[id] = a.cap;
        s.cancelled[id] = false;
        s.gen[id] +%= 1;
        if (s.gen[id] == 0) s.gen[id] = 1; // zero means "names nothing"
        s.budget[id] = 0;
        s.reserve[id] = a.reserve;
        s.makeRunnable(id, .spawn);
        return .{ .task = id, .gen = s.gen[id] };
    }

    pub fn release(s: *Sched, id: TaskId) void {
        s.disarm(id);
        s.live[id] = false;
        s.cancelled[id] = false;
        s.gen[id] +%= 1; // invalidates every outstanding token for this slot
    }

    // ---------------------------------------------------------- cancellation


    /// Cancel is two independent guarantees:
    ///   safety   - budget = 0, so the task structurally cannot do more body
    ///              work even if the app's handling is buggy. The reserve is
    ///              untouched, so the unwind stays funded.
    ///   liveness - makeRunnable, so a task parked in epoll notices now
    ///              rather than at its deadline.
    /// Sticky, idempotent, and not a value anything can swallow.
    pub fn cancel(s: *Sched, tok: CancelTok) bool {
        if (tok.gen == 0 or !s.live[tok.task] or s.gen[tok.task] != tok.gen) {
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

    /// See `Fault`. Returns null when this task still has the authority to run.
    pub fn faultOf(s: *const Sched, id: TaskId) ?Fault {
        if (s.cancelled[id]) return .cancelled;
        return null;
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

    // --------------------------------------------------------- quota handle

    pub fn assignQuota(s: *Sched, id: TaskId, q: quota.Id) void {
        s.quota_of[id] = q;
    }

    /// Draw execution units into this task's grant. The scheduler asks the
    /// tree; it does not implement conservation itself.
    pub fn topUp(s: *Sched, id: TaskId, tree: *quota.Tree, want: i64) bool {
        if (s.cancelled[id]) return false;
        if (!tree.draw(s.quota_of[id], want)) {
            s.top_up_denials += 1;
            return false;
        }
        s.budget[id] += want;
        s.top_ups += 1;
        return true;
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
        // `slot != nil` is not the test for "already armed". A task parked on
        // the overflow list has no slot, so re-arming one skipped the unlink
        // and pushed it onto the list a second time. The push sets
        // `t.next = s.overflow` while `s.overflow` is already this task, so
        // the list became a ring pointing at itself, and every later walk of
        // it -- the sweep in `expire`, the scan in `timeoutMs` -- ran forever.
        //
        // Nothing reached overflow at the default deadline of 3000ms, which
        // fits inside the wheel's 4096ms revolution, so the path sat unvisited
        // until a test asked for a longer one and the server spun at 100% CPU
        // with a connection half-answered.
        if (s.isLinked(id)) {
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
            s.overflows += 1;
            return;
        }
        const slot: u32 = @intCast(@as(usize, @intCast(@max(0, at_ms))) % wheel_slots);
        s.link(id, slot);
        s.armed += 1;
    }

    pub fn disarm(s: *Sched, id: TaskId) void {
        if (!s.isLinked(id)) return;
        s.unlink(id);
    }

    /// Whether this task is on the wheel or on the overflow list. Both count:
    /// an overflow entry has no slot but is very much armed.
    fn isLinked(s: *const Sched, id: TaskId) bool {
        const t = &s.timer[id];
        return t.slot != nil or t.prev != nil or t.next != nil or s.overflow == id;
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
    /// Spend execution units. Returns false if it will not fit -- the caller
    /// transitions to cleanup rather than being killed.
    ///
    /// No account, no attribution, no idea what the work was. Telemetry with
    /// application-defined labels is the application's business.
    pub fn charge(s: *Sched, id: TaskId, tree: *quota.Tree, units: i64) bool {
        if (units > s.cap[id]) return false;
        if (units > s.budget[id]) {
            const want = @min(@max(units, s.grant_size), s.cap[id]);
            if (!s.topUp(id, tree, want)) return false;
        }
        s.budget[id] -= units;
        s.cap[id] -= units;
        return true;
    }


    /// Spend from the reserve. Only the unwind path calls this, which is the
    /// entire enforcement: no other function in the program touches it.
    pub fn chargeReserve(s: *Sched, id: TaskId, units: i64) void {
        s.reserve[id] -= units;
    }
};
