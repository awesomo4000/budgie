//! Time as a parameter.
//!
//! Two modes, one interface. In `.real` mode this is `clock_gettime` and
//! nothing else. In `.virtual` mode time only moves when something advances
//! it, which buys three things:
//!
//!   - Deterministic replay. Nothing branches on wall clock, so identical
//!     inputs produce an identical execution every run.
//!   - Time travel. A virtual run jumps straight to the next scheduled event,
//!     so an hour of connection timeouts takes milliseconds.
//!   - Simulated slowness without being slow: shrink a budget rather than
//!     burning real CPU.
//!
//! The rule that makes it work is unchanged from the two-currency design:
//! units enforce, nanoseconds observe, and no control flow reads the clock
//! except to decide how long to block -- which in virtual mode is a jump.

const std = @import("std");
const linux = std.os.linux;

pub const Mode = enum { real, virtual };

pub const Clock = struct {
    mode: Mode = .real,
    v_ns: i64 = 0,
    reads: u64 = 0,
    jumps: u64 = 0,

    pub fn real() Clock {
        return .{ .mode = .real };
    }

    pub fn virtualAt(start_ns: i64) Clock {
        return .{ .mode = .virtual, .v_ns = start_ns };
    }

    pub fn ns(c: *Clock) i64 {
        c.reads += 1;
        if (c.mode == .virtual) return c.v_ns;
        var ts: linux.timespec = undefined;
        _ = linux.clock_gettime(.MONOTONIC, &ts);
        return @as(i64, @intCast(ts.sec)) * 1_000_000_000 + @as(i64, @intCast(ts.nsec));
    }

    pub fn ms(c: *Clock) i64 {
        return @divTrunc(c.ns(), 1_000_000);
    }

    /// Virtual only. Jump forward; the caller decides how far, which is what
    /// makes a simulation skip idle time instead of sleeping through it.
    pub fn advanceTo(c: *Clock, at_ns: i64) void {
        std.debug.assert(c.mode == .virtual);
        if (at_ns > c.v_ns) {
            c.v_ns = at_ns;
            c.jumps += 1;
        }
    }

    pub fn advanceBy(c: *Clock, d_ns: i64) void {
        c.advanceTo(c.v_ns + d_ns);
    }
};
