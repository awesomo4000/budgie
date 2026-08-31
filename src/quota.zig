//! Conservation trees over a resource. Not a scheduler concept.
//!
//! This was inside `sched.zig` and did not belong there. Nothing here is about
//! execution: it is a parent/child tree where a draw deducts from a node AND
//! from every ancestor, so a child can never outspend its parent. The unit is
//! whatever the caller says it is.
//!
//! The seL4 framing is the check that this is not arbitrary. There, a
//! scheduling context and an untyped are DIFFERENT object types derived from
//! the same capability tree, and the kernel has no idea what the memory is
//! for. A driver's buffer budget is not a scheduler concept; it is a cap the
//! driver holds. Our tree is that common root, and it was welded to the
//! scheduler by accident of where it got written.
//!
//! Refill semantics differ per unit and that is a POLICY FIELD, not three
//! mechanisms:
//!
//!   .periodic   CPU-like. Balance is restored on a clock. Exhaustion means
//!               "wait for the period" -- a rate.
//!   .on_return  memory-like. Balance rises when something is freed, never on
//!               a timer. Exhaustion is TERMINAL: parking on a refill that
//!               will never come is a deadlock, which is why buffer exhaustion
//!               is a 503 rather than a yield.
//!   .never      flow-like. Nothing refills; the source is asked to stop.
//!
//! That distinction is load-bearing. Getting it wrong turns a resource limit
//! into a hang.

const std = @import("std");

pub const Id = u8;
pub const max_nodes = 16;
pub const none: Id = 255;
pub const unlimited: i64 = std.math.maxInt(i64) / 4;

pub const Refill = enum { periodic, on_return, never };

pub const Node = struct {
    parent: Id = none,
    /// Units restored at each refill. `unlimited` for an unmetered class.
    quota: i64 = 0,
    balance: i64 = 0,
    refill: Refill = .periodic,
    period_ms: i64 = 100,
    next_refill: i64 = 0,
    /// How much this node has handed down, and how often it said no.
    granted: i64 = 0,
    denials: u64 = 0,
    tag: []const u8 = "",
};

pub const Tree = struct {
    nodes: [max_nodes]Node = @splat(.{}),
    draws: u64 = 0,
    denials: u64 = 0,

    pub fn define(t: *Tree, id: Id, parent: Id, quota: i64, refill: Refill, period_ms: i64, tag: []const u8) void {
        // `.on_return` is not implemented and selecting it silently deadlocks,
        // so it fails here instead. `refillPeriodic` skips non-periodic nodes,
        // which leaves `giveBack` as the only thing that could restore the
        // balance, and `giveBack` has no callers anywhere: nothing returns
        // budget when a task ends. A node declared this way drains to zero on
        // first use and never recovers, and every draw after that is denied
        // for a reason nobody could see.
        //
        // The idea is worth keeping, which is why the variant stays. Wiring it
        // means deciding where unspent budget goes when a task is released,
        // and that is a design question rather than a missing line.
        if (refill == .on_return) @panic("quota: Refill.on_return needs a caller for giveBack; nothing returns budget yet");
        t.nodes[id] = .{
            .parent = parent,
            .quota = quota,
            .balance = quota,
            .refill = refill,
            .period_ms = period_ms,
            .next_refill = 0,
            .tag = tag,
        };
    }

    /// Take `want` units from `id` and from every ancestor.
    ///
    /// All-or-nothing: a partial walk is rolled back, so a denial never
    /// silently spends a parent's balance.
    pub fn draw(t: *Tree, id: Id, want: i64) bool {
        if (want <= 0) return true;
        if (id == none) {
            t.denials += 1;
            return false; // explicitly unparented is a denial, not a blank cheque
        }
        var chain: [8]Id = undefined;
        var n: usize = 0;
        var cur = id;
        while (cur != none and n < chain.len) : (n += 1) {
            if (t.nodes[cur].balance < want) {
                t.nodes[cur].denials += 1;
                t.denials += 1;
                return false;
            }
            chain[n] = cur;
            cur = t.nodes[cur].parent;
        }
        for (chain[0..n]) |c| {
            t.nodes[c].balance -= want;
            t.nodes[c].granted += want;
        }
        t.draws += 1;
        return true;
    }

    /// Give units back up the chain. Only meaningful for `.on_return` nodes;
    /// a periodic node's balance is restored by the clock instead.
    pub fn giveBack(t: *Tree, id: Id, units: i64) void {
        var cur = id;
        while (cur != none) {
            const nd = &t.nodes[cur];
            if (nd.refill == .on_return) nd.balance = @min(nd.balance + units, nd.quota);
            cur = nd.parent;
        }
    }

    pub fn refillPeriodic(t: *Tree, now_ms: i64) void {
        for (&t.nodes) |*nd| {
            if (nd.quota == 0 or nd.refill != .periodic) continue;
            if (nd.next_refill == 0) nd.next_refill = now_ms + nd.period_ms;
            if (now_ms >= nd.next_refill) {
                nd.balance = nd.quota;
                nd.next_refill = now_ms + nd.period_ms;
            }
        }
    }

    /// Soonest refill of a node that is currently exhausted, so a caller can
    /// avoid sleeping past the moment a throttled class becomes usable again.
    pub fn nextRefillMs(t: *const Tree, now_ms: i64) ?i64 {
        var best: ?i64 = null;
        for (t.nodes) |nd| {
            if (nd.quota == 0 or nd.quota >= unlimited) continue;
            if (nd.refill != .periodic or nd.balance > 0) continue;
            if (nd.next_refill == 0) continue;
            if (best == null or nd.next_refill < best.?) best = nd.next_refill;
        }
        if (best) |b| return @max(0, b - now_ms);
        return null;
    }
};
