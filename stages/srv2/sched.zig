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
pub const max_tasks = 8192;

/// Why a task became runnable. The app switches on this instead of scanning
/// for its own timeout condition.
pub const Wake = enum { spawn, io, deadline };

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

    // --- runnable ring ---
    ring: [max_tasks]TaskId = undefined,
    head: usize = 0,
    len: usize = 0,
    queued: [max_tasks]bool = @splat(false),
    wake: [max_tasks]Wake = @splat(.spawn),

    // --- budgets (caller-pays; reserve is untouchable by the body) ---
    budget: [max_tasks]i64 = @splat(0),
    reserve: [max_tasks]i64 = @splat(0),

    // --- deadlines: timer wheel ---
    timer: [max_tasks]Timer = @splat(.{}),
    bucket: [wheel_slots]u32 = @splat(nil),
    occupied: [wheel_slots / 64]u64 = @splat(0),
    cur: i64 = 0, // wheel has fired everything strictly before this ms
    armed: usize = 0,
    overflow: u32 = nil, // deadlines beyond one revolution

    // stats
    fires: u64 = 0,
    rearms: u64 = 0,

    // ------------------------------------------------------------ lifecycle

    pub fn admit(s: *Sched, id: TaskId, budget: i64, reserve: i64) void {
        s.live[id] = true;
        s.budget[id] = budget;
        s.reserve[id] = reserve;
        s.makeRunnable(id, .spawn);
    }

    pub fn release(s: *Sched, id: TaskId) void {
        s.disarm(id);
        s.live[id] = false;
    }

    /// Refresh the body budget without touching the reserve.
    pub fn refund(s: *Sched, id: TaskId, budget: i64, reserve: i64) void {
        s.budget[id] = budget;
        s.reserve[id] = reserve;
    }

    // ------------------------------------------------------------- runnable

    pub fn makeRunnable(s: *Sched, id: TaskId, why: Wake) void {
        if (!s.live[id] or s.queued[id]) return;
        s.queued[id] = true;
        s.wake[id] = why;
        s.ring[(s.head + s.len) % max_tasks] = id;
        s.len += 1;
    }

    pub fn popRunnable(s: *Sched) ?TaskId {
        while (s.len > 0) {
            const id = s.ring[s.head];
            s.head = (s.head + 1) % max_tasks;
            s.len -= 1;
            s.queued[id] = false;
            if (s.live[id]) return id;
        }
        return null;
    }

    pub fn reasonFor(s: *const Sched, id: TaskId) Wake {
        return s.wake[id];
    }

    pub fn anyRunnable(s: *const Sched) bool {
        return s.len > 0;
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

    /// Spend from the reserve. Only the unwind path calls this, which is the
    /// entire enforcement: no other function in the program touches it.
    pub fn chargeReserve(s: *Sched, id: TaskId, units: i64) void {
        s.reserve[id] -= units;
    }
};
