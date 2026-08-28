//! The Darwin half of the application's platform seam.
//!
//! `sys.zig` documents the contract these functions implement; the notes below
//! are only about where Darwin genuinely differs from Linux, which is the
//! interesting part -- most of it is the same call with a different return
//! convention, and that is handled once in `wrap`.

const std = @import("std");
const posix = std.posix;
const c = std.c;

/// Widen a libc `c_int` result into the negative-means-failure convention the
/// Linux syscalls return natively, so `sysErr` reads the same on both.
fn wrap(rc: c_int) usize {
    return @bitCast(@as(isize, rc));
}

/// The BSDs put a length byte in front of the family and shrink the family to
/// one byte to pay for it. This is a real layout difference, not a spelling
/// one: getting it wrong binds to a garbage port rather than failing.
pub const SockAddrIn = extern struct {
    len: u8 = @sizeOf(@This()),
    family: u8 = c.AF.INET,
    port: u16,
    addr: u32,
    zero: [8]u8 = @splat(0),
};

/// There is no `SOCK_NONBLOCK` here, so the flag costs a second call.
pub fn tcpSocketNonblock() usize {
    const fd = c.socket(c.AF.INET, c.SOCK.STREAM, 0);
    if (fd < 0) return wrap(fd);
    setNonblock(fd);
    return wrap(fd);
}

pub fn setNonblock(fd: c_int) void {
    const flags = c.fcntl(fd, c.F.GETFL, @as(c_int, 0));
    if (flags < 0) return;
    _ = c.fcntl(fd, c.F.SETFL, flags | @as(c_int, @bitCast(c.O{ .NONBLOCK = true })));
}

/// Nagle off. Load generators want each request on the wire immediately, or
/// they measure Nagle rather than the server.
pub fn setNoDelay(fd: i32) void {
    const one: c_int = 1;
    _ = c.setsockopt(fd, c.IPPROTO.TCP, c.TCP.NODELAY, @ptrCast(&one), @sizeOf(c_int));
}

/// Sleep for up to `ns`. May return early if a signal arrives; the caller is
/// responsible for deciding whether that matters.
pub fn sleepRelNs(ns: u64) void {
    var ts: c.timespec = .{ .sec = @intCast(ns / 1_000_000_000), .nsec = @intCast(ns % 1_000_000_000) };
    _ = c.nanosleep(&ts, null);
}

/// SO_LINGER, so a test can close with a RST instead of a FIN.
pub fn setLinger(fd: i32, opt: *const anyopaque, len: u32) usize {
    return wrap(c.setsockopt(fd, c.SOL.SOCKET, c.SO.LINGER, @ptrCast(opt), len));
}

/// Shrink the receive buffer, so a test can make the peer's writes block
/// without having to send megabytes.
pub fn setRecvBuf(fd: i32, bytes: c_int) void {
    _ = c.setsockopt(fd, c.SOL.SOCKET, c.SO.RCVBUF, @ptrCast(&bytes), @sizeOf(c_int));
}

pub fn setSendBuf(fd: i32, bytes: c_int) void {
    _ = c.setsockopt(fd, c.SOL.SOCKET, c.SO.SNDBUF, @ptrCast(&bytes), @sizeOf(c_int));
}

pub fn setReuseAddr(fd: i32) void {
    const one: c_int = 1;
    _ = c.setsockopt(fd, c.SOL.SOCKET, c.SO.REUSEADDR, @ptrCast(&one), @sizeOf(c_int));
}

pub fn bind(fd: i32, addr: *const SockAddrIn) usize {
    return wrap(c.bind(fd, @ptrCast(addr), @sizeOf(SockAddrIn)));
}

pub fn listen(fd: i32, backlog: u31) usize {
    return wrap(c.listen(fd, backlog));
}

pub fn getsockname(fd: i32, addr: *SockAddrIn) usize {
    var len: c.socklen_t = @sizeOf(SockAddrIn);
    return wrap(c.getsockname(fd, @ptrCast(addr), &len));
}

/// No `accept4`, so every accepted connection pays an extra `fcntl` pair that
/// the Linux build does not. It is two syscalls per connection, not per
/// request, so it does not show up under load.
pub fn acceptNonblock(listener: i32) usize {
    const fd = c.accept(listener, null, null);
    if (fd < 0) return wrap(fd);
    setNonblock(fd);
    return wrap(fd);
}

/// A blocking TCP socket, for a client that wants to wait rather than poll.
pub fn tcpSocket() usize {
    return wrap(c.socket(c.AF.INET, c.SOCK.STREAM, 0));
}

pub fn connect(fd: i32, addr: *const SockAddrIn) usize {
    return wrap(c.connect(fd, @ptrCast(addr), @sizeOf(SockAddrIn)));
}

/// Half-close: stop sending, keep receiving. `how` is 1 for SHUT_WR.
pub fn shutdown(fd: i32, how: i32) usize {
    return wrap(c.shutdown(fd, how));
}

/// Whether a failed call merely means "not right now".
///
/// Every I/O call here reports failure as a negative return, which lumps
/// EAGAIN in with EPIPE and ECONNRESET. Those mean opposite things. EAGAIN
/// says park and wait; the others say the peer is gone and the connection
/// should be reclaimed. Treating a terminal error as would-block parks a task
/// on a dead descriptor, and since it keeps waking on I/O rather than on its
/// deadline, nothing ever reclaims it: measured at 397 buffers and 401 tasks
/// held after 400 reset connections.
///
/// Darwin reports -1 and puts the code in a thread-local, so this reads what
/// the immediately preceding call left behind. Call it right after the check.
pub fn wouldBlock(rc: usize) bool {
    _ = rc;
    const e = c._errno().*;
    return e == @intFromEnum(c.E.AGAIN) or e == @intFromEnum(c.E.INTR);
}

pub fn read(fd: i32, buf: [*]u8, len: usize) usize {
    return @bitCast(c.read(fd, buf, len));
}

pub fn write(fd: i32, buf: [*]const u8, len: usize) usize {
    return @bitCast(c.write(fd, buf, len));
}

pub fn close(fd: i32) void {
    _ = c.close(fd);
}

const timeval = extern struct { sec: isize, usec: isize };
const itimerval = extern struct { it_interval: timeval, it_value: timeval };

/// Declared here because `std.c` does not re-export it. Plain libc otherwise.
extern "c" fn setitimer(which: c_int, new: *const itimerval, old: ?*itimerval) c_int;

pub fn armIntervalTimer(ms: u64) void {
    const v: itimerval = .{
        .it_interval = .{ .sec = @intCast(ms / 1000), .usec = @intCast((ms % 1000) * 1000) },
        .it_value = .{ .sec = @intCast(ms / 1000), .usec = @intCast((ms % 1000) * 1000) },
    };
    _ = setitimer(0, &v, null);
}

/// There is no `/proc`, so this is the Mach task info call. `getrusage` would
/// have been less code but reports *peak* RSS, and every RSS figure in this
/// project is a steady-state reading, where peak is the wrong quantity.
pub fn rssKb() u64 {
    var info: c.mach_task_basic_info = undefined;
    var count: c.mach_msg_type_number_t = c.MACH.TASK.BASIC.INFO_COUNT;
    const rc = c.task_info(c.mach_task_self(), c.MACH.TASK.BASIC.INFO, @ptrCast(&info), &count);
    if (rc != 0) return 0;
    return @intCast(info.resident_size / 1024);
}

/// `getrusage` reports the same two quantities Linux reads out of
/// `/proc/self/stat`, and std still wraps it, so this needs no extern.
pub fn cpuMs() u64 {
    const ru = posix.getrusage(0); // 0 = RUSAGE_SELF
    const u = @as(u64, @intCast(ru.utime.sec)) * 1000 + @as(u64, @intCast(@divTrunc(ru.utime.usec, 1000)));
    const s = @as(u64, @intCast(ru.stime.sec)) * 1000 + @as(u64, @intCast(@divTrunc(ru.stime.usec, 1000)));
    return u + s;
}
