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
const builtin = @import("builtin");
const posix = std.posix;

/// Monotonic nanoseconds, straight from the OS.
///
/// On Linux and Darwin `posix.system` is the platform dispatch: it resolves to
/// `std.c` where libc is linked and to the native OS module otherwise, so no
/// switch and no direct reference to libc is needed.
///
/// Windows is the exception and needs one, which is why the switch below
/// exists at all. There is no `clock_gettime` outside libc there, and pulling
/// libc in for one function would make every other target link it too. The
/// counter is `QueryPerformanceCounter`, whose frequency is fixed at boot, so
/// it is read once and kept. `posix.errno` normalises the two
/// return conventions -- Linux returns a negative errno, Darwin returns -1 and
/// sets a global -- which is the other half of what a hand-written switch here
/// would have had to get right. It is the same call std makes internally for
/// `Io.Clock`, which is deliberately not used here: that wants an `Io` threaded
/// through every caller, and a clock you must be handed is exactly what the
/// `Clock` type below exists to avoid.
///
/// MONOTONIC and never REALTIME. Nothing here may observe time going backwards
/// because an operator set the wall clock.
pub fn monotonicNs() i64 {
    if (comptime builtin.os.tag == .windows) return windowsNs();
    var ts: posix.timespec = undefined;
    if (posix.errno(posix.system.clock_gettime(posix.CLOCK.MONOTONIC, &ts)) != .SUCCESS) return 0;
    return @as(i64, @intCast(ts.sec)) * 1_000_000_000 + @as(i64, @intCast(ts.nsec));
}

/// Ticks to nanoseconds without overflowing and without losing the remainder.
///
/// The obvious `ticks * 1_000_000_000 / hz` overflows a u64 after about
/// eighteen seconds at a 10 MHz counter, which is the frequency Windows
/// actually reports on current hardware. Splitting into whole seconds plus
/// remainder keeps full resolution and cannot overflow until the machine has
/// been up for centuries.
fn windowsNs() i64 {
    // `RtlQueryPerformance*` from ntdll rather than the kernel32 wrappers,
    // which is where 0.16 keeps them. Both return BOOL and write through a
    // pointer; a false return means no high-resolution counter, and returning
    // zero then matches what the posix path does when `clock_gettime` fails.
    const ntdll = std.os.windows.ntdll;
    const cached = struct {
        var hz: i64 = 0;
    };
    if (cached.hz == 0) {
        var f: std.os.windows.LARGE_INTEGER = undefined;
        if (ntdll.RtlQueryPerformanceFrequency(&f) == .FALSE) return 0;
        cached.hz = f;
    }
    if (cached.hz <= 0) return 0;
    var c: std.os.windows.LARGE_INTEGER = undefined;
    if (ntdll.RtlQueryPerformanceCounter(&c) == .FALSE) return 0;
    const ticks: i64 = c;
    const whole = @divTrunc(ticks, cached.hz);
    const rest = @mod(ticks, cached.hz);
    return whole * 1_000_000_000 + @divTrunc(rest * 1_000_000_000, cached.hz);
}

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
        return monotonicNs();
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
