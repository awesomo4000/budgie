//! The platform seam for the server.
//!
//! `reactor.zig` needed a backend for the wakeup primitive; this is the same
//! idea for everything else the application asks of the OS -- sockets, the
//! tick timer, and the two process-introspection numbers the stats line
//! prints. The server above it names no OS.
//!
//! Every call here keeps the raw-syscall convention the Linux build already
//! used: a `usize` that is negative when bitcast to `isize` means failure.
//! Linux returns that natively; on Darwin the libc `-1` is widened to match,
//! so `sysErr` reads the same on both and the call sites did not have to
//! change shape.

const std = @import("std");
const builtin = @import("builtin");
const is_linux = builtin.os.tag == .linux;
const linux = std.os.linux;
const c = std.c;

pub fn sysErr(rc: usize) bool {
    return @as(isize, @bitCast(rc)) < 0;
}

/// Widen a libc `c_int` result into the raw-syscall convention above.
fn wrap(rc: c_int) usize {
    return @bitCast(@as(isize, rc));
}

/// `sockaddr_in`, which is genuinely a different struct on the two platforms:
/// the BSDs put a length byte in front of the family and shrink the family to
/// one byte to pay for it. Getting this wrong binds to a garbage port rather
/// than failing, so it is worth spelling out.
pub const SockAddrIn = if (is_linux) extern struct {
    family: u16 = linux.AF.INET,
    port: u16,
    addr: u32,
    zero: [8]u8 = @splat(0),
} else extern struct {
    len: u8 = @sizeOf(@This()),
    family: u8 = c.AF.INET,
    port: u16,
    addr: u32,
    zero: [8]u8 = @splat(0),
};

/// Loopback, in network byte order on both platforms.
pub const loopback: u32 = 0x0100007f;

pub fn hostToNetPort(p: u16) u16 {
    return std.mem.nativeToBig(u16, p);
}

pub fn netToHostPort(p: u16) u16 {
    return std.mem.bigToNative(u16, p);
}

/// A non-blocking TCP socket. Linux folds `SOCK_NONBLOCK` into the socket
/// type; Darwin has no such flag and needs the extra `fcntl`.
pub fn tcpSocketNonblock() usize {
    if (is_linux) {
        return linux.socket(linux.AF.INET, linux.SOCK.STREAM | linux.SOCK.NONBLOCK, 0);
    }
    const fd = c.socket(c.AF.INET, c.SOCK.STREAM, 0);
    if (fd < 0) return wrap(fd);
    setNonblock(fd);
    return wrap(fd);
}

fn setNonblock(fd: c_int) void {
    const flags = c.fcntl(fd, c.F.GETFL, @as(c_int, 0));
    if (flags < 0) return;
    _ = c.fcntl(fd, c.F.SETFL, flags | @as(c_int, @bitCast(c.O{ .NONBLOCK = true })));
}

pub fn setReuseAddr(fd: i32) void {
    const one: c_int = 1;
    if (is_linux) {
        _ = linux.setsockopt(fd, linux.SOL.SOCKET, linux.SO.REUSEADDR, @ptrCast(&one), @sizeOf(c_int));
    } else {
        _ = c.setsockopt(fd, c.SOL.SOCKET, c.SO.REUSEADDR, @ptrCast(&one), @sizeOf(c_int));
    }
}

pub fn bind(fd: i32, addr: *const SockAddrIn) usize {
    if (is_linux) return linux.bind(fd, @ptrCast(addr), @sizeOf(SockAddrIn));
    return wrap(c.bind(fd, @ptrCast(addr), @sizeOf(SockAddrIn)));
}

pub fn listen(fd: i32, backlog: u31) usize {
    if (is_linux) return linux.listen(fd, backlog);
    return wrap(c.listen(fd, backlog));
}

/// Read back the bound address, which is how the server learns which port it
/// actually got when it was asked for port 0.
pub fn getsockname(fd: i32, addr: *SockAddrIn) usize {
    if (is_linux) {
        var len: linux.socklen_t = @sizeOf(SockAddrIn);
        return linux.getsockname(fd, @ptrCast(addr), &len);
    }
    var len: c.socklen_t = @sizeOf(SockAddrIn);
    return wrap(c.getsockname(fd, @ptrCast(addr), &len));
}

/// Accept one connection, already non-blocking. `accept4` is Linux-only, so
/// Darwin pays an extra `fcntl` per accepted connection.
pub fn acceptNonblock(listener: i32) usize {
    if (is_linux) return linux.accept4(listener, null, null, linux.SOCK.NONBLOCK);
    const fd = c.accept(listener, null, null);
    if (fd < 0) return wrap(fd);
    setNonblock(fd);
    return wrap(fd);
}

pub fn read(fd: i32, buf: [*]u8, len: usize) usize {
    if (is_linux) return linux.read(fd, buf, len);
    return @bitCast(c.read(fd, buf, len));
}

pub fn write(fd: i32, buf: [*]const u8, len: usize) usize {
    if (is_linux) return linux.write(fd, buf, len);
    return @bitCast(c.write(fd, buf, len));
}

pub fn close(fd: i32) void {
    if (is_linux) {
        _ = linux.close(fd);
    } else {
        _ = c.close(fd);
    }
}

const timeval = extern struct { sec: isize, usec: isize };
const itimerval = extern struct { it_interval: timeval, it_value: timeval };

// Declared here rather than taken from `std.c`, which does not re-export
// either of them. Both are plain libc and are only referenced on the Darwin
// path, so the Linux build never resolves them.
extern "c" fn setitimer(which: c_int, new: *const itimerval, old: ?*itimerval) c_int;
extern "c" fn getrusage(who: c_int, usage: *c.rusage) c_int;

/// The tick. A repeating interval timer delivering SIGALRM, which is what
/// makes the stall watchdog able to observe a task that never yields.
pub fn armIntervalTimer(ms: u64) void {
    const v: itimerval = .{
        .it_interval = .{ .sec = @intCast(ms / 1000), .usec = @intCast((ms % 1000) * 1000) },
        .it_value = .{ .sec = @intCast(ms / 1000), .usec = @intCast((ms % 1000) * 1000) },
    };
    if (is_linux) {
        _ = linux.syscall3(.setitimer, 0, @intFromPtr(&v), 0);
    } else {
        _ = setitimer(0, &v, null);
    }
}

/// Resident set size in KiB.
///
/// Linux reads `/proc/self/statm`, unchanged from the original. Darwin has no
/// `/proc`, so this is the Mach task info call -- `getrusage` would have been
/// less code but reports peak RSS, and every RSS number in this project is a
/// steady-state reading where peak would be the wrong quantity.
pub fn rssKb() u64 {
    if (is_linux) {
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
    var info: c.mach_task_basic_info = undefined;
    var count: c.mach_msg_type_number_t = c.MACH.TASK.BASIC.INFO_COUNT;
    const rc = c.task_info(
        c.mach_task_self(),
        c.MACH.TASK.BASIC.INFO,
        @ptrCast(&info),
        &count,
    );
    if (rc != 0) return 0;
    return @intCast(info.resident_size / 1024);
}

/// User + system CPU time in milliseconds.
///
/// Linux keeps reading `/proc/self/stat` so the measured build is untouched;
/// Darwin uses `getrusage`, which reports the same two quantities.
pub fn cpuMs() u64 {
    if (is_linux) {
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
    var ru: c.rusage = undefined;
    if (getrusage(0, &ru) != 0) return 0; // 0 = RUSAGE_SELF
    const u = @as(u64, @intCast(ru.utime.sec)) * 1000 + @as(u64, @intCast(@divTrunc(ru.utime.usec, 1000)));
    const s = @as(u64, @intCast(ru.stime.sec)) * 1000 + @as(u64, @intCast(@divTrunc(ru.stime.usec, 1000)));
    return u + s;
}
