//! The platform seam for the application: sockets, the tick timer, and the two
//! process-introspection numbers the stats line prints. The server above it
//! names no OS.
//!
//! This is the same shape as `reactor.zig` and its backends -- one selector, a
//! per-platform file either side of it -- and it exists for the same reason.
//! `std.posix` in 0.16 no longer wraps `socket`, `bind`, `listen`, `accept`,
//! `close` or `write`; they moved behind `std.Io`, which wants an `Io` threaded
//! through every caller and is exactly the dependency this project is built to
//! do without. So the wrappers have to live somewhere, and they live here
//! rather than being scattered through the server.
//!
//! The contract every implementation keeps:
//!
//!   - Calls that can fail return a `usize` that is negative when bitcast to
//!     `isize`. Linux returns that natively; Darwin's libc `-1` is widened to
//!     match. `sysErr` is the only way anything above reads the result, so the
//!     two platforms are indistinguishable to the caller.
//!   - `SockAddrIn` is whatever the platform's `sockaddr_in` actually is. It
//!     is not the same struct on both, so it is never spelled out above.
//!   - Sockets come back non-blocking, however that has to be arranged.
//!
//! The `clock` module is deliberately not part of this: `posix.system` still
//! dispatches `clock_gettime` on its own, so that one needs no seam at all.

const std = @import("std");
const builtin = @import("builtin");

/// The platform implementation. Selected here and nowhere else.
///
/// Linux and Darwin only. The kqueue backend under `reactor.zig` would work on
/// the other BSDs, but `rssKb` here is a Mach call, so claiming them would be
/// claiming something untested and, in that one function, wrong.
pub const impl = switch (builtin.os.tag) {
    .linux => @import("sys_linux.zig"),
    .macos, .ios, .tvos, .watchos, .visionos => @import("sys_darwin.zig"),
    else => @compileError("no platform implementation in sys.zig for this OS"),
};

// The contract, re-exported one name at a time. Zig 0.16 has no
// `usingnamespace`, and the explicit list is the better form regardless: it is
// the whole surface the application is allowed to reach the OS through, in one
// screen, and adding to it is a deliberate act.
pub const SockAddrIn = impl.SockAddrIn;
pub const tcpSocketNonblock = impl.tcpSocketNonblock;
pub const setReuseAddr = impl.setReuseAddr;
pub const bind = impl.bind;
pub const listen = impl.listen;
pub const getsockname = impl.getsockname;
pub const acceptNonblock = impl.acceptNonblock;
pub const tcpSocket = impl.tcpSocket;
pub const connect = impl.connect;
pub const shutdown = impl.shutdown;
pub const read = impl.read;
pub const write = impl.write;
pub const close = impl.close;
pub const armIntervalTimer = impl.armIntervalTimer;
pub const rssKb = impl.rssKb;
pub const cpuMs = impl.cpuMs;

// --- platform-independent, so they stay here rather than being written twice

pub fn sysErr(rc: usize) bool {
    return @as(isize, @bitCast(rc)) < 0;
}

/// Loopback, in network byte order, which is the same bit pattern everywhere.
pub const loopback: u32 = 0x0100007f;

pub fn hostToNetPort(p: u16) u16 {
    return std.mem.nativeToBig(u16, p);
}

pub fn netToHostPort(p: u16) u16 {
    return std.mem.bigToNative(u16, p);
}
