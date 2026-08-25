//! Per-phase telemetry with application-defined labels.
//!
//! This lived in `sched.zig` as an `Account` enum reading
//! `{ accept, parse, work, write, cleanup, background }` -- which are HTTP
//! server phases. A CPU scheduler that knows what "parse" means has stopped
//! being a CPU scheduler. The labels belong beside the application.
//!
//! Two currencies, unchanged: units are the enforced, deterministic quantity
//! and nanoseconds are observed and never read by control flow. Keeping that
//! rule is what makes budget exhaustion land at the identical point on every
//! replay.

const std = @import("std");

/// Set false to compile out every clock read on the observation path.
pub const observe_ns = true;

/// Labels are the APPLICATION's. This set happens to describe an HTTP server;
/// a different app names different phases and the scheduler is unaffected.
pub const Label = enum {
    accept,
    parse,
    work,
    write,
    cleanup,
    background,

    pub const count = @typeInfo(Label).@"enum".fields.len;
};

pub const Book = struct {
    units: [Label.count]i64 = @splat(0),
    ns: [Label.count]i64 = @splat(0),
    calls: [Label.count]u64 = @splat(0),
    denied: [Label.count]u64 = @splat(0),

    pub fn charged(b: *Book, l: Label, units: i64, ok: bool) void {
        const i = @intFromEnum(l);
        b.calls[i] += 1;
        if (ok) b.units[i] += units else b.denied[i] += 1;
    }

    pub fn observe(b: *Book, l: Label, ns_elapsed: i64) void {
        if (!observe_ns) return;
        b.ns[@intFromEnum(l)] += ns_elapsed;
    }

    pub fn observedTotal(b: *const Book) i64 {
        var t: i64 = 0;
        for (b.ns) |v| t += v;
        return t;
    }
};
