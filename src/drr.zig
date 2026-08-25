//! Deficit Round Robin over byte flows.
//!
//! The fairness property we want, stated precisely:
//!
//!   1. a flow that wants less than its share is never affected by one that
//!      wants more
//!   2. N flows all wanting more than their share converge to 1/N each
//!   3. unused share is redistributed, not wasted -- one greedy flow alone
//!      gets the whole machine
//!   4. a flow returning from idle reclaims its share IMMEDIATELY, and cannot
//!      cash in a backlog accumulated while it was quiet
//!
//! That is max-min fairness with work conservation, and DRR is the standard
//! O(1) algorithm for it: round robin plus one integer per flow. No floating
//! point, no virtual clock, no per-packet sorting.
//!
//! Property 4 is the one that is easy to get wrong and is why idle flows are
//! RESET rather than credited: an idle flow that accumulated deficit would
//! come back and burst at the expense of everyone else, which is exactly the
//! "calm down when the other one wants their share" clause failing.
//!
//! This file knows nothing about sockets, buffers or io_uring. It is a
//! bookkeeper: `charge` says whether a flow may keep going, `advanceRound`
//! says which flows became eligible again. Enforcement -- for us, whether to
//! re-arm a connection's recv -- belongs to the caller.

const std = @import("std");
const sched = @import("sched.zig");

pub const TaskId = sched.TaskId;
pub const max_flows = sched.max_tasks;

/// When does a round boundary occur?
///
/// This is the ONLY thing that legitimately differs between backends, and
/// getting it wrong is what made one quantum mean different things on epoll
/// and io_uring. It is a seam, not a fork: the deficit accounting below is
/// identical either way.
pub const RoundPolicy = enum {
    /// Advance whenever the loop is about to block. Simple, and WRONG across
    /// backends: the rate becomes `quantum x block_rate`, and block rate is an
    /// emergent property of each reactor's wake pattern.
    on_block,
    /// Textbook DRR: a round ends once every backlogged flow has had a turn.
    /// This is the RIGHT idea and this implementation of it DOES NOT WORK.
    ///
    /// Two failures, both measured:
    ///   * comparing `served_this_round` against the live `n_active` is
    ///     trivially true after a round clears `active`, so a round fires per
    ///     charge -- 15,014 rounds, zero throttles, no effect at all.
    ///   * snapshotting the round size instead deadlocks: a throttled flow
    ///     cannot be charged, so the count never reaches the snapshot, so
    ///     rounds stop (rounds=1) and nothing resumes. Throughput collapsed
    ///     from 55k to 3k.
    ///
    /// The missing piece is a notion of "backlogged" that survives throttling
    /// -- a flow with data waiting is still owed a turn even though it cannot
    /// be served. That needs the reactor to say "this flow has pending bytes",
    /// which drr.zig deliberately cannot see. Resolving that is the real work.
    on_service,
    /// Token bucket with a SHARED, EXTERNAL refill clock (the tick).
    ///
    /// The others fail for one reason: classic DRR controls the DEQUEUE -- the
    /// scheduler chooses when to serve a flow, so "a round ends when every
    /// backlogged flow has had a turn" is something it can observe. We control
    /// the ARRIVAL: the peer chooses when to send, and a throttled flow simply
    /// stops being served, so any round condition phrased in terms of service
    /// either never fires or deadlocks. Measured both ways.
    ///
    /// Refilling on a fixed external clock sidesteps it. Each active flow gets
    /// `quantum` bytes per tick, so the per-flow rate is `quantum / tick` --
    /// controlled, predictable, and identical on every backend, because the
    /// tick is not an emergent property of the reactor's wake pattern.
    on_tick,
};

// Status, so the next attempt starts from evidence rather than from scratch:
//
//   on_block   WORKS on the completion backend (polite share 5% -> 61-67%).
//              Does not transfer to readiness: no quantum lands near 50%.
//   on_service BROKEN on both. Classic DRR observes the DEQUEUE; we control
//              the ARRIVAL, so a round condition phrased in terms of service
//              either fires per charge (15,014 rounds, no throttles) or
//              deadlocks (rounds=1, throughput 55k -> 3k).
//   on_tick    Right idea -- an external shared clock removes the dependency
//              on each reactor's wake pattern -- but on the readiness backend
//              the greedy flow gets exactly 6 requests at EVERY quantum from
//              512 to 4096. Quantum-independent, so it is a resume bug and
//              not a tuning problem. Untested on the completion backend.
//
// The next step is to instrument the resume path, not to tune a constant.

pub const Drr = struct {
    /// Bytes this flow may still consume this round. Starts at `quantum`.
    deficit: [max_flows]i64 = @splat(0),
    /// Over its allowance and waiting for the next round.
    throttled: [max_flows]bool = @splat(false),
    /// Has consumed anything this round. Only active flows are credited;
    /// this is what implements property 4.
    active: [max_flows]bool = @splat(false),
    live: [max_flows]bool = @splat(false),

    quantum: i64 = 65536,
    policy: RoundPolicy = .on_service,
    /// Distinct flows charged since the last round boundary.
    served_this_round: usize = 0,
    served_mark: [max_flows]bool = @splat(false),
    /// How many distinct flows were served in the PREVIOUS round. The round
    /// size must be a snapshot: `advanceRound` clears `active`, so comparing
    /// against the live `n_active` is trivially satisfied by the first charge
    /// after a round and fires a round per charge (measured: 15,014 rounds,
    /// zero throttles).
    round_size: usize = 0,
    n_live: usize = 0,
    n_throttled: usize = 0,
    n_active: usize = 0,

    rounds: u64 = 0,
    throttles: u64 = 0,
    credits: u64 = 0,
    bytes_charged: u64 = 0,

    pub fn admit(d: *Drr, id: TaskId) void {
        if (!d.live[id]) d.n_live += 1;
        d.live[id] = true;
        d.deficit[id] = d.quantum;
        if (d.throttled[id]) d.n_throttled -= 1;
        d.throttled[id] = false;
        if (d.active[id]) d.n_active -= 1;
        d.active[id] = false;
    }

    pub fn release(d: *Drr, id: TaskId) void {
        if (d.live[id]) d.n_live -= 1;
        if (d.throttled[id]) d.n_throttled -= 1;
        if (d.active[id]) d.n_active -= 1;
        d.live[id] = false;
        d.throttled[id] = false;
        d.active[id] = false;
        d.deficit[id] = 0;
    }

    /// Consume `bytes` of this flow's allowance.
    /// Returns true if the flow may keep receiving, false if it is now over
    /// its share and must wait for the next round.
    pub fn charge(d: *Drr, id: TaskId, bytes: usize) bool {
        if (!d.live[id]) return true;
        if (!d.active[id]) {
            d.active[id] = true;
            d.n_active += 1;
        }
        if (!d.served_mark[id]) {
            d.served_mark[id] = true;
            d.served_this_round += 1;
        }
        d.deficit[id] -= @intCast(bytes);
        d.bytes_charged += bytes;
        if (d.deficit[id] > 0) return true;
        if (!d.throttled[id]) {
            d.throttled[id] = true;
            d.n_throttled += 1;
            d.throttles += 1;
        }
        return false;
    }

    pub fn isThrottled(d: *const Drr, id: TaskId) bool {
        return d.throttled[id];
    }

    /// A flow that went quiet. Reset rather than credit: it must not bank
    /// allowance while idle, or it returns and bursts.
    pub fn idle(d: *Drr, id: TaskId) void {
        if (!d.live[id]) return;
        if (d.active[id]) {
            d.active[id] = false;
            d.n_active -= 1;
        }
        d.deficit[id] = d.quantum;
    }

    /// Credit every throttled flow one quantum. Returns how many became
    /// eligible again, so the caller can re-arm exactly those.
    ///
    /// Work conservation comes from WHEN the caller advances rounds, not from
    /// this function: if only one flow is active, the caller should advance as
    /// soon as that flow is the only thing blocking progress, so it is
    /// credited continuously and runs at full rate.
    pub fn advanceRound(d: *Drr, out: []TaskId) usize {
        d.rounds += 1;
        if (d.served_this_round > 0) d.round_size = d.served_this_round;
        d.served_this_round = 0;
        @memset(&d.served_mark, false);
        var n: usize = 0;
        for (d.live, 0..) |lv, i| {
            if (!lv) continue;
            const id: TaskId = @intCast(i);
            if (d.active[id]) {
                // Backlogged: credit one quantum, capped so a flow that used
                // less than its share cannot BANK the remainder and burst
                // later. That cap is property 4.
                d.deficit[id] = @min(d.deficit[id] + d.quantum, d.quantum);
                d.credits += 1;
                d.active[id] = false;
                d.n_active -= 1;
            } else {
                // Quiet for a whole round: reset, do not accumulate.
                d.deficit[id] = d.quantum;
            }
            if (d.throttled[id] and d.deficit[id] > 0) {
                d.throttled[id] = false;
                d.n_throttled -= 1;
                if (n < out.len) {
                    out[n] = id;
                    n += 1;
                }
            }
        }
        return n;
    }

    /// Flows currently over their share. When this equals the number of
    /// active flows, nothing can proceed and a round must advance.
    pub fn allThrottled(d: *const Drr) bool {
        return d.n_active > 0 and d.n_throttled >= d.n_active;
    }

    /// Should the caller advance a round now?
    pub fn dueForRound(d: *const Drr) bool {
        return switch (d.policy) {
            .on_block => d.n_throttled > 0,
            // Every backlogged flow has had its turn, or nothing can proceed.
            .on_service => (d.round_size > 0 and d.served_this_round >= d.round_size) or d.allThrottled(),
            // Driven by the caller on tick, never by `dueForRound`.
            .on_tick => d.allThrottled(),
        };
    }
};
