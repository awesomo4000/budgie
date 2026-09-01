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
/// Linux, Darwin and Windows. The kqueue backend under `reactor.zig` would work
/// on the other BSDs, but `rssKb` here is a Mach call, so claiming them would be
/// claiming something untested and, in that one function, wrong.
pub const impl = switch (builtin.os.tag) {
    .linux => @import("sys_linux.zig"),
    .macos, .ios, .tvos, .watchos, .visionos => @import("sys_darwin.zig"),
    .windows => @import("sys_windows.zig"),
    else => @compileError("no platform implementation in sys.zig for this OS"),
};

// The contract, re-exported one name at a time. Zig 0.16 has no
// `usingnamespace`, and the explicit list is the better form regardless: it is
// the whole surface the application is allowed to reach the OS through, in one
// screen, and adding to it is a deliberate act.
pub const SockAddrIn = impl.SockAddrIn;
pub const tcpSocketNonblock = impl.tcpSocketNonblock;
pub const setReuseAddr = impl.setReuseAddr;
pub const setLinger = impl.setLinger;
pub const setRecvBuf = impl.setRecvBuf;
pub const setSendBuf = impl.setSendBuf;
pub const setNonblock = impl.setNonblock;
pub const setNoDelay = impl.setNoDelay;
pub const sleepRelNs = impl.sleepRelNs;
pub const bind = impl.bind;
pub const listen = impl.listen;
pub const getsockname = impl.getsockname;
pub const acceptNonblock = impl.acceptNonblock;
pub const tcpSocket = impl.tcpSocket;
pub const connect = impl.connect;
pub const shutdown = impl.shutdown;
pub const wouldBlock = impl.wouldBlock;
pub const read = impl.read;
pub const write = impl.write;
pub const close = impl.close;
pub const armIntervalTimer = impl.armIntervalTimer;
pub const rssKb = impl.rssKb;
pub const cpuMs = impl.cpuMs;


/// Ignore SIGPIPE, so that writing to a peer that has hung up returns EPIPE
/// instead of killing the process.
///
/// The default disposition for SIGPIPE is to terminate, which for a server is
/// a remote denial of service and not a diagnostic. A client only has to send
/// a request and reset the connection while the answer is being written.
/// Measured before this existed: 400 request-then-RST cycles against
/// `app/server.zig` killed it after roughly three, exit status 141, which is
/// 128 plus signal 13.
///
/// Every write in this project already checks its return value, so EPIPE lands
/// on a path that closes the connection and reclaims the task.
///
/// Nothing to do on Windows, and this is one of the rare cases where the
/// platform is simply better. There is no SIGPIPE: a send to a peer that has
/// gone away returns `WSAECONNRESET` as an ordinary failed call, which is what
/// the code above already handles. The default disposition problem does not
/// exist, so there is no default to change.
pub fn ignoreSigpipe() void {
    if (comptime builtin.os.tag == .windows) return;
    var act: std.posix.Sigaction = .{
        .handler = .{ .handler = std.posix.SIG.IGN },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(.PIPE, &act, null);
}

pub const PollFd = impl.PollFd;
pub const poll_in = impl.poll_in;
pub const poll_out = impl.poll_out;
pub const pollFd = impl.pollFd;
pub const pollSet = impl.pollSet;

/// Wait for one socket to become readable, or for the timeout to pass. True
/// means readable.
///
/// A seam rather than a direct `std.posix.poll` call because that does not
/// compile for Windows in 0.16: `std.posix.pollfd` aliases a `ws2_32.pollfd`
/// which the types-only ws2_32 module does not declare. The Windows side uses
/// `WSAPoll`, which is the same idea for sockets.
///
/// Error and timeout are the same answer here. Every caller treated them the
/// same anyway, and collapsing them removes a distinction nobody acted on.
pub fn waitReadable(fd: i32, timeout_ms: i32) bool {
    if (comptime builtin.os.tag == .windows) return impl.pollReadable(fd, timeout_ms);
    var p = [_]std.posix.pollfd{.{ .fd = fd, .events = std.posix.POLL.IN, .revents = 0 }};
    const n = std.posix.poll(&p, timeout_ms) catch return false;
    return n > 0;
}

// --- platform-independent, so they stay here rather than being written twice

pub fn sysErr(rc: usize) bool {
    return @as(isize, @bitCast(rc)) < 0;
}

/// Loopback, in network byte order, which is the same bit pattern everywhere.
pub const loopback: u32 = 0x0100007f;

/// Sleep for `ms`, measured against the monotonic clock rather than against
/// however many times a signal interrupted the attempt.
///
/// The obvious spellings both get this wrong in opposite directions. A bare
/// `nanosleep` returns early when a signal arrives and reports a shorter sleep
/// than asked for. `std.posix.poll` retries EINTR internally with the *full*
/// timeout again, so a process taking a signal every 20ms and asking for 375ms
/// can sleep for seconds: measured at 2311ms for a 375ms request, against a
/// server whose own tick was the thing doing the interrupting.
///
/// Neither is acceptable for a test that is timing a server's deadline, so
/// this loops against an absolute deadline and lets the clock decide.
pub fn sleepMs(ms: u64) void {
    const clock = @import("clock.zig");
    const deadline = clock.monotonicNs() + @as(i64, @intCast(ms)) * 1_000_000;
    while (true) {
        const remaining = deadline - clock.monotonicNs();
        if (remaining <= 0) return;
        sleepRelNs(@intCast(remaining));
    }
}

pub fn hostToNetPort(p: u16) u16 {
    return std.mem.nativeToBig(u16, p);
}

pub fn netToHostPort(p: u16) u16 {
    return std.mem.bigToNative(u16, p);
}
