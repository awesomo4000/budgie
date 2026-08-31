//! The Linux half of the application's platform seam.
//!
//! Everything here is the raw syscall layer, unchanged from the build the
//! measurements in README.md were taken on. `sys.zig` documents the contract
//! these functions implement; the notes below are only about what is specific
//! to Linux.

const std = @import("std");
const linux = std.os.linux;

/// Linux orders the family first and has no length byte.
pub const SockAddrIn = extern struct {
    family: u16 = linux.AF.INET,
    port: u16,
    addr: u32,
    zero: [8]u8 = @splat(0),
};

/// `SOCK_NONBLOCK` folds into the socket type, so this is one call.
pub fn tcpSocketNonblock() usize {
    return linux.socket(linux.AF.INET, linux.SOCK.STREAM | linux.SOCK.NONBLOCK, 0);
}

/// Non-blocking mode on an existing descriptor. `tcpSocketNonblock` folds
/// this into the socket call; a client that connects first needs it after.
pub fn setNonblock(fd: i32) void {
    _ = linux.fcntl(fd, 4, 0o4000); // F_SETFL, O_NONBLOCK
}

/// Nagle off. Load generators want each request on the wire immediately, or
/// they measure Nagle rather than the server.
pub fn setNoDelay(fd: i32) void {
    const one: c_int = 1;
    _ = linux.setsockopt(fd, 6, 1, @ptrCast(&one), @sizeOf(c_int)); // IPPROTO_TCP, TCP_NODELAY
}

/// Sleep for up to `ns`. May return early if a signal arrives; the caller is
/// responsible for deciding whether that matters.
pub fn sleepRelNs(ns: u64) void {
    var ts: linux.timespec = .{ .sec = @intCast(ns / 1_000_000_000), .nsec = @intCast(ns % 1_000_000_000) };
    _ = linux.nanosleep(&ts, null);
}

/// SO_LINGER, so a test can close with a RST instead of a FIN.
pub fn setLinger(fd: i32, opt: *const anyopaque, len: u32) usize {
    return linux.setsockopt(fd, linux.SOL.SOCKET, 13, @ptrCast(opt), len); // SO_LINGER
}

/// Shrink the receive buffer, so a test can make the peer's writes block
/// without having to send megabytes.
pub fn setRecvBuf(fd: i32, bytes: c_int) void {
    _ = linux.setsockopt(fd, linux.SOL.SOCKET, linux.SO.RCVBUF, @ptrCast(&bytes), @sizeOf(c_int));
}

pub fn setSendBuf(fd: i32, bytes: c_int) void {
    _ = linux.setsockopt(fd, linux.SOL.SOCKET, linux.SO.SNDBUF, @ptrCast(&bytes), @sizeOf(c_int));
}

pub fn setReuseAddr(fd: i32) void {
    const one: c_int = 1;
    _ = linux.setsockopt(fd, linux.SOL.SOCKET, linux.SO.REUSEADDR, @ptrCast(&one), @sizeOf(c_int));
}

pub fn bind(fd: i32, addr: *const SockAddrIn) usize {
    return linux.bind(fd, @ptrCast(addr), @sizeOf(SockAddrIn));
}

pub fn listen(fd: i32, backlog: u31) usize {
    return linux.listen(fd, backlog);
}

pub fn getsockname(fd: i32, addr: *SockAddrIn) usize {
    var len: linux.socklen_t = @sizeOf(SockAddrIn);
    return linux.getsockname(fd, @ptrCast(addr), &len);
}

/// `accept4` takes the non-blocking flag directly, so an accepted connection
/// costs exactly one syscall.
pub fn acceptNonblock(listener: i32) usize {
    return linux.accept4(listener, null, null, linux.SOCK.NONBLOCK);
}

/// A blocking TCP socket, for a client that wants to wait rather than poll.
pub fn tcpSocket() usize {
    return linux.socket(linux.AF.INET, linux.SOCK.STREAM, 0);
}

pub fn connect(fd: i32, addr: *const SockAddrIn) usize {
    return linux.connect(fd, @ptrCast(addr), @sizeOf(SockAddrIn));
}

/// Half-close: stop sending, keep receiving. `how` is 1 for SHUT_WR.
pub fn shutdown(fd: i32, how: i32) usize {
    return linux.shutdown(fd, how);
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
pub fn wouldBlock(rc: usize) bool {
    const e = -@as(isize, @bitCast(rc));
    return e == @intFromEnum(linux.E.AGAIN) or e == @intFromEnum(linux.E.INTR);
}

pub fn read(fd: i32, buf: [*]u8, len: usize) usize {
    return linux.read(fd, buf, len);
}

pub fn write(fd: i32, buf: [*]const u8, len: usize) usize {
    return linux.write(fd, buf, len);
}

pub fn close(fd: i32) void {
    _ = linux.close(fd);
}

const timeval = extern struct { sec: isize, usec: isize };
const itimerval = extern struct { it_interval: timeval, it_value: timeval };

pub fn armIntervalTimer(ms: u64) void {
    const v: itimerval = .{
        .it_interval = .{ .sec = @intCast(ms / 1000), .usec = @intCast((ms % 1000) * 1000) },
        .it_value = .{ .sec = @intCast(ms / 1000), .usec = @intCast((ms % 1000) * 1000) },
    };
    _ = linux.syscall3(.setitimer, 0, @intFromPtr(&v), 0);
}

fn sysErr(rc: usize) bool {
    return @as(isize, @bitCast(rc)) < 0;
}

/// Field two of `/proc/self/statm` is resident pages.
pub fn rssKb() u64 {
    var buf: [256]u8 = undefined;
    const fd = linux.open("/proc/self/statm", .{ .ACCMODE = .RDONLY }, 0);
    if (sysErr(fd)) return 0;
    defer _ = linux.close(@intCast(fd));
    const n = linux.read(@intCast(fd), &buf, buf.len);
    if (sysErr(n)) return 0;
    var it = std.mem.tokenizeScalar(u8, buf[0..n], ' ');
    _ = it.next();
    const pages = std.fmt.parseInt(u64, it.next() orelse "0", 10) catch 0;
    return pages * 4; // 4 KiB pages
}

/// utime + stime from `/proc/self/stat`, fields 14 and 15 -- counted from
/// after the last ')' because the comm field can itself contain parentheses.
pub fn cpuMs() u64 {
    var buf: [1024]u8 = undefined;
    const fd = linux.open("/proc/self/stat", .{ .ACCMODE = .RDONLY }, 0);
    if (sysErr(fd)) return 0;
    defer _ = linux.close(@intCast(fd));
    const n = linux.read(@intCast(fd), &buf, buf.len);
    if (sysErr(n)) return 0;
    const close_paren = std.mem.lastIndexOfScalar(u8, buf[0..n], ')') orelse return 0;
    var it = std.mem.tokenizeScalar(u8, buf[close_paren + 2 .. n], ' ');
    var i: usize = 0;
    var total: u64 = 0;
    while (it.next()) |tok| : (i += 1) {
        if (i == 11 or i == 12) total += std.fmt.parseInt(u64, tok, 10) catch 0;
        if (i > 12) break;
    }
    return total * 10; // USER_HZ = 100
}

// --- the poll seam, for the load generators

/// `poll` over a whole set, which the load generators need and `waitReadable`
/// does not cover. Same three names on every platform; the struct differs,
/// which is why callers build it through `pollFd` rather than by hand.
pub const PollFd = std.posix.pollfd;
pub const poll_in: i16 = std.posix.POLL.IN;
pub const poll_out: i16 = std.posix.POLL.OUT;

pub fn pollFd(fd: i32) i32 {
    return fd;
}

pub fn pollSet(fds: []PollFd, timeout_ms: i32) usize {
    return std.posix.poll(fds, timeout_ms) catch 0;
}
