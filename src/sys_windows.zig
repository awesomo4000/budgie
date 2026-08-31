//! The platform layer for Windows, against ws2_32 and kernel32 directly.
//!
//! No libc. Zig 0.16's `std.os.windows.ws2_32` is 248 lines of types with no
//! function declarations at all, and `kernel32.zig` has exactly one extern
//! function in it, so the externs below are ours. That is the same situation
//! this project already met on the other two platforms, where `std.posix` had
//! been stripped to 54 functions with no socket, bind, listen, accept, close
//! or write among them. The types ARE worth taking from std, and are: `AF`,
//! `SOCK`, `SOL`, `SO`, `TCP`, `IPPROTO` and `linger` all come from there, so
//! the constants cannot drift from what the platform actually defines.
//!
//! Three things here are genuinely different rather than differently spelled,
//! and they are the interesting part of the port.
//!
//! A socket is not a file descriptor. `SOCKET` is a kernel handle, and the
//! error value is `INVALID_SOCKET`, which is all-bits-set rather than -1. The
//! rest of this project passes descriptors as `i32` and asks `sysErr` whether
//! the return was negative, so everything here narrows to `i32` and returns
//! -1 on failure. That keeps one convention across three platforms; the cost
//! is that a handle above 2^31 would break it, which cannot happen for sockets
//! in practice because Windows allocates them low and they are documented as
//! safe to truncate for exactly this reason.
//!
//! Errors are not `errno`. `WSAGetLastError` is per-thread state set by the
//! last socket call, which is the same shape as Darwin's `_errno()` and not at
//! all the same as Linux's negative return. `wouldBlock` reads it.
//!
//! There is no SIGPIPE, and that is a simplification rather than a gap. See
//! `ignoreSigpipe` below.

const std = @import("std");
const ws2 = std.os.windows.ws2_32;

const SOCKET = usize;
const INVALID_SOCKET: SOCKET = ~@as(usize, 0);
const SOCKET_ERROR: c_int = -1;

const WSAEWOULDBLOCK: c_int = 10035;
const WSAEINTR: c_int = 10004;
const FIONBIO: c_ulong = 0x8004667e;

pub const SockAddrIn = extern struct {
    family: u16 = ws2.AF.INET,
    port: u16,
    addr: u32,
    zero: [8]u8 = @splat(0),
};

// ------------------------------------------------------------- the externs

/// The externs, in one namespace so the symbol names stay honest and the
/// wrappers below can reuse them.
const c = struct {
    extern "ws2_32" fn WSAStartup(wVersionRequested: u16, lpWSAData: *WSADATA) callconv(.winapi) c_int;
    extern "ws2_32" fn WSAGetLastError() callconv(.winapi) c_int;
    extern "ws2_32" fn socket(af: c_int, t: c_int, protocol: c_int) callconv(.winapi) SOCKET;
    extern "ws2_32" fn closesocket(s: SOCKET) callconv(.winapi) c_int;
    extern "ws2_32" fn bind(s: SOCKET, name: *const anyopaque, namelen: c_int) callconv(.winapi) c_int;
    extern "ws2_32" fn listen(s: SOCKET, backlog: c_int) callconv(.winapi) c_int;
    extern "ws2_32" fn accept(s: SOCKET, addr: ?*anyopaque, addrlen: ?*c_int) callconv(.winapi) SOCKET;
    extern "ws2_32" fn connect(s: SOCKET, name: *const anyopaque, namelen: c_int) callconv(.winapi) c_int;
    extern "ws2_32" fn shutdown(s: SOCKET, how: c_int) callconv(.winapi) c_int;
    extern "ws2_32" fn recv(s: SOCKET, buf: [*]u8, len: c_int, flags: c_int) callconv(.winapi) c_int;
    extern "ws2_32" fn send(s: SOCKET, buf: [*]const u8, len: c_int, flags: c_int) callconv(.winapi) c_int;
    extern "ws2_32" fn setsockopt(s: SOCKET, level: c_int, optname: c_int, optval: ?*const anyopaque, optlen: c_int) callconv(.winapi) c_int;
    extern "ws2_32" fn getsockname(s: SOCKET, name: *anyopaque, namelen: *c_int) callconv(.winapi) c_int;
    extern "ws2_32" fn ioctlsocket(s: SOCKET, cmd: c_long, argp: *c_ulong) callconv(.winapi) c_int;

    extern "kernel32" fn Sleep(dwMilliseconds: u32) callconv(.winapi) void;
    extern "kernel32" fn GetProcessTimes(
        hProcess: ?*anyopaque,
        lpCreationTime: *FILETIME,
        lpExitTime: *FILETIME,
        lpKernelTime: *FILETIME,
        lpUserTime: *FILETIME,
    ) callconv(.winapi) c_int;
    extern "kernel32" fn K32GetProcessMemoryInfo(
        Process: ?*anyopaque,
        ppsmemCounters: *PROCESS_MEMORY_COUNTERS,
        cb: u32,
    ) callconv(.winapi) c_int;
};

const FILETIME = extern struct { low: u32, high: u32 };

const PROCESS_MEMORY_COUNTERS = extern struct {
    cb: u32,
    PageFaultCount: u32,
    PeakWorkingSetSize: usize,
    WorkingSetSize: usize,
    QuotaPeakPagedPoolUsage: usize,
    QuotaPagedPoolUsage: usize,
    QuotaPeakNonPagedPoolUsage: usize,
    QuotaNonPagedPoolUsage: usize,
    PagefileUsage: usize,
    PeakPagefileUsage: usize,
};

const WSADATA = extern struct {
    wVersion: u16,
    wHighVersion: u16,
    iMaxSockets: u16,
    iMaxUdpDg: u16,
    lpVendorInfo: ?[*]u8,
    szDescription: [257]u8,
    szSystemStatus: [129]u8,
};

// ------------------------------------------------------------- conventions

/// Winsock must be started before any socket call, once per process. There is
/// no equivalent on the other two platforms, so it hides here rather than
/// appearing in the contract and forcing every caller to know about it. It is
/// idempotent and cheap after the first call.
var started: bool = false;

fn ensureStarted() void {
    if (started) return;
    var data: WSADATA = undefined;
    _ = c.WSAStartup(0x0202, &data); // 2.2
    started = true;
}

/// Narrow a SOCKET to the `i32` the rest of the project passes around, mapping
/// `INVALID_SOCKET` to -1 so `sysErr` reads it the same way it reads a Linux
/// negative errno or a Darwin -1.
fn wrapSock(s: SOCKET) usize {
    if (s == INVALID_SOCKET) return @bitCast(@as(isize, -1));
    return @bitCast(@as(isize, @intCast(s)));
}

fn wrap(rc: c_int) usize {
    return @bitCast(@as(isize, rc));
}

fn sock(fd: i32) SOCKET {
    return @intCast(fd);
}

/// Whether the last socket call failed only because it would have blocked.
///
/// Reads thread-local state set by the call, the same shape as Darwin's
/// `_errno()` and unlike Linux, where the error is in the return value. EINTR
/// is in here on the other platforms because a signal can interrupt a syscall;
/// Windows has no such thing, so `WSAEINTR` only appears if something called
/// `WSACancelBlockingCall`, which nothing here does. It is checked anyway
/// because leaving it out would be assuming that stays true.
pub fn wouldBlock(rc: usize) bool {
    _ = rc;
    const e = c.WSAGetLastError();
    return e == WSAEWOULDBLOCK or e == WSAEINTR;
}

// ----------------------------------------------------------------- sockets

pub fn tcpSocket() usize {
    ensureStarted();
    return wrapSock(c.socket(ws2.AF.INET, ws2.SOCK.STREAM, 0));
}

pub fn tcpSocketNonblock() usize {
    const rc = tcpSocket();
    if (@as(isize, @bitCast(rc)) < 0) return rc;
    setNonblock(@intCast(rc));
    return rc;
}

/// There is no `SOCK_NONBLOCK` and no `fcntl`. `FIONBIO` is the whole
/// mechanism, and unlike `O_NONBLOCK` it is not a flag you can read back.
pub fn setNonblock(fd: c_int) void {
    var on: c_ulong = 1;
    _ = c.ioctlsocket(sock(fd), @bitCast(FIONBIO), &on);
}

pub fn setReuseAddr(fd: i32) void {
    // Deliberately SO_REUSEADDR and nothing else. On Windows this permits
    // binding a port another socket is actively listening on, which is a
    // stronger and more dangerous thing than the POSIX meaning; the tests want
    // the POSIX behaviour of reusing a port in TIME_WAIT, and get it, but it
    // is worth knowing they are not the same option.
    const one: c_int = 1;
    _ = c.setsockopt(sock(fd), ws2.SOL.SOCKET, ws2.SO.REUSEADDR, &one, @sizeOf(c_int));
}

pub fn setNoDelay(fd: i32) void {
    const one: c_int = 1;
    _ = c.setsockopt(sock(fd), ws2.IPPROTO.TCP, ws2.TCP.NODELAY, &one, @sizeOf(c_int));
}

pub fn setLinger(fd: i32, opt: *const anyopaque, len: u32) usize {
    return wrap(c.setsockopt(sock(fd), ws2.SOL.SOCKET, ws2.SO.LINGER, opt, @intCast(len)));
}

pub fn setRecvBuf(fd: i32, bytes: c_int) void {
    _ = c.setsockopt(sock(fd), ws2.SOL.SOCKET, ws2.SO.RCVBUF, &bytes, @sizeOf(c_int));
}

pub fn setSendBuf(fd: i32, bytes: c_int) void {
    _ = c.setsockopt(sock(fd), ws2.SOL.SOCKET, ws2.SO.SNDBUF, &bytes, @sizeOf(c_int));
}

pub fn bind(fd: i32, addr: *const SockAddrIn) usize {
    return wrap(c.bind(sock(fd), addr, @sizeOf(SockAddrIn)));
}

pub fn listen(fd: i32, backlog: u31) usize {
    return wrap(c.listen(sock(fd), backlog));
}

pub fn getsockname(fd: i32, addr: *SockAddrIn) usize {
    var len: c_int = @sizeOf(SockAddrIn);
    return wrap(c.getsockname(sock(fd), addr, &len));
}

/// Accepted sockets do NOT inherit non-blocking mode, so it is set here. That
/// is the same as Darwin needing a second call and unlike Linux's `accept4`.
pub fn acceptNonblock(listener: i32) usize {
    const s = c.accept(sock(listener), null, null);
    if (s == INVALID_SOCKET) return @bitCast(@as(isize, -1));
    const fd: i32 = @intCast(s);
    setNonblock(fd);
    return wrapSock(s);
}

pub fn connect(fd: i32, addr: *const SockAddrIn) usize {
    return wrap(c.connect(sock(fd), addr, @sizeOf(SockAddrIn)));
}

pub fn shutdown(fd: i32, how: i32) usize {
    return wrap(c.shutdown(sock(fd), how));
}

/// `recv` and `send`, not `read` and `write`. A Windows socket handle is not a
/// file handle and `ReadFile` on one is a different code path with different
/// semantics, so the socket calls are the honest ones. The names stay `read`
/// and `write` because that is what the contract in `sys.zig` calls them.
pub fn read(fd: i32, buf: [*]u8, len: usize) usize {
    return wrap(c.recv(sock(fd), buf, @intCast(@min(len, std.math.maxInt(c_int))), 0));
}

pub fn write(fd: i32, buf: [*]const u8, len: usize) usize {
    return wrap(c.send(sock(fd), buf, @intCast(@min(len, std.math.maxInt(c_int))), 0));
}

pub fn close(fd: i32) void {
    _ = c.closesocket(sock(fd));
}

// ------------------------------------------------------ time and the process

/// Millisecond resolution, which is coarser than the other two platforms
/// offer. `sleepMs` in `sys.zig` loops against the monotonic clock, so the
/// coarseness costs accuracy on a single call and not on the total.
pub fn sleepRelNs(ns: u64) void {
    const ms = ns / 1_000_000;
    c.Sleep(@intCast(@min(ms, std.math.maxInt(u32))));
}

/// Not implemented, and the reason is the interesting part.
///
/// The other two platforms arm an interval timer that raises a signal, and the
/// signal handler sets `should_yield` so a running task can be asked to stop
/// hogging the loop. Windows has no signals. The equivalent is a thread that
/// sleeps and sets the same atomic, or a waitable timer queued to an APC, and
/// either is a real design decision rather than a translation, because a
/// thread setting a flag is not a preemption point in the way a signal is.
///
/// Left unimplemented rather than faked: a server built here would run with no
/// tick, which is a thing worth noticing rather than a thing to paper over.
pub fn armIntervalTimer(interval_ms: u64) void {
    _ = interval_ms;
}

pub fn rssKb() u64 {
    var pmc: PROCESS_MEMORY_COUNTERS = undefined;
    pmc.cb = @sizeOf(PROCESS_MEMORY_COUNTERS);
    if (c.K32GetProcessMemoryInfo(std.os.windows.GetCurrentProcess(), &pmc, pmc.cb) == 0) return 0;
    return pmc.WorkingSetSize / 1024;
}

pub fn cpuMs() u64 {
    var creation: FILETIME = undefined;
    var exit: FILETIME = undefined;
    var kernel: FILETIME = undefined;
    var user: FILETIME = undefined;
    if (c.GetProcessTimes(std.os.windows.GetCurrentProcess(), &creation, &exit, &kernel, &user) == 0) return 0;
    // FILETIME counts 100ns intervals, so 10,000 of them per millisecond.
    const k = (@as(u64, kernel.high) << 32 | kernel.low) / 10_000;
    const u = (@as(u64, user.high) << 32 | user.low) / 10_000;
    return k + u;
}
