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
const win = std.os.windows;
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
    extern "ws2_32" fn WSAPoll(fdArray: [*]WSAPOLLFD, fds: c_ulong, timeout: c_int) callconv(.winapi) c_int;
    extern "ws2_32" fn getsockopt(s: SOCKET, level: c_int, optname: c_int, optval: *anyopaque, optlen: *c_int) callconv(.winapi) c_int;
    extern "ws2_32" fn ioctlsocket(s: SOCKET, cmd: c_long, argp: *c_ulong) callconv(.winapi) c_int;

    extern "kernel32" fn CreateTimerQueueTimer(
        phNewTimer: *?*anyopaque,
        TimerQueue: ?*anyopaque,
        Callback: *const fn (?*anyopaque, u8) callconv(.winapi) void,
        Parameter: ?*anyopaque,
        DueTime: u32,
        Period: u32,
        Flags: u32,
    ) callconv(.winapi) win.BOOL;

    extern "kernel32" fn DeleteTimerQueueTimer(
        TimerQueue: ?*anyopaque,
        Timer: ?*anyopaque,
        CompletionEvent: ?*anyopaque,
    ) callconv(.winapi) win.BOOL;

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

/// Sockets already accepted by the reactor, waiting to be collected.
///
/// This is the one place the Windows port could not keep the same shape as the
/// other two, and it is worth saying why rather than burying it.
///
/// A listening socket has no readiness on IOCP. There is nothing to ask about
/// it: a zero-byte `WSARecv` against one fails with `WSAENOTCONN`, because
/// there is no connection to receive on. The only completion-model way to wait
/// for an inbound connection is `AcceptEx`, and `AcceptEx` does not report that
/// a connection is available, it performs the accept. By the time anything
/// knows a client arrived, the socket already exists.
///
/// So on Windows the accept happens in the reactor and `acceptNonblock` is a
/// collection point rather than a system call. The queue is the handoff. The
/// server above sees the same thing it sees everywhere else: a task wakes, asks
/// for a socket, and gets one or gets a would-block.
///
/// Room for several, because `AcceptEx` can be posted more than once and the
/// reactor may collect several completions from one `wait` before the accept
/// task runs again.
///
/// Entries carry the listener they arrived on, and that is not bookkeeping for
/// its own sake. This server listens twice: once for requests and once, on the
/// next port up, for the control surface, and they are accepted by different
/// tasks with different priorities. A single undifferentiated queue lets the
/// request-accept loop take a control connection and treat it as HTTP, which is
/// exactly what happened: the control generator connected, its socket was
/// handed to the wrong handler, and it sat waiting for a reply that was never
/// going to come.
const accept_ring = 32;

const Accepted = struct { listener: i32 = -1, fd: i32 = -1 };

var accepted: [accept_ring]Accepted = @splat(.{});
var accepted_n: usize = 0;

/// Hand a socket the reactor accepted to the queue, tagged with the listener it
/// came in on. Called by the IOCP backend and by nothing else. Returns false if
/// there is no room, which tells the caller to close the socket rather than
/// drop it on the floor.
pub fn pushAccepted(listener: i32, fd: i32) bool {
    if (accepted_n == accept_ring) return false;
    accepted[accepted_n] = .{ .listener = listener, .fd = fd };
    accepted_n += 1;
    return true;
}

pub fn acceptedPending() usize {
    return accepted_n;
}

/// Take the oldest queued socket for this listener, keeping the rest in order.
/// A linear scan over at most thirty-two entries, which is cheaper than the
/// bookkeeping a per-listener ring would need for two listeners.
fn takeAccepted(listener: i32) ?i32 {
    var i: usize = 0;
    while (i < accepted_n) : (i += 1) {
        if (accepted[i].listener != listener) continue;
        const fd = accepted[i].fd;
        var j = i;
        while (j + 1 < accepted_n) : (j += 1) accepted[j] = accepted[j + 1];
        accepted_n -= 1;
        return fd;
    }
    return null;
}

/// Close every queued socket for a listener that is going away, so a shutdown
/// does not leave connections open with nobody owning them.
pub fn dropAccepted(listener: i32) void {
    while (takeAccepted(listener)) |fd| close(fd);
}

/// Take the next socket the reactor accepted on this listener, or fall back to
/// a real `accept`.
///
/// The fallback is not decoration. Anything that listens without a reactor --
/// a test with its own loop, a tool -- still gets working behaviour, and a
/// mixed setup works too, because `AcceptEx` and `accept` draw from the same
/// backlog. When neither has anything, this reports a would-block the way the
/// platform would, so `wouldBlock` above answers correctly and the caller needs
/// no special case.
pub fn acceptNonblock(listener: i32) usize {
    if (takeAccepted(listener)) |fd| {
        // Set again here rather than trusting where it came from. `AcceptEx`
        // sockets are created non-blocking, but the completion path then sets
        // `SO_UPDATE_ACCEPT_CONTEXT`, which copies properties from the listener
        // onto the socket, and the documented list of what that touches is not
        // the same as the list of what it actually touches. A socket that
        // silently reverted to blocking does not fail loudly: the server writes
        // to a full buffer, the whole loop stops, and every other connection
        // waits with no error anywhere. Costing one call to rule that out is a
        // good trade.
        setNonblock(fd);
        return @bitCast(@as(isize, fd));
    }

    // Accepted sockets do NOT inherit non-blocking mode, so it is set here.
    // That is the same as Darwin needing a second call and unlike Linux's
    // `accept4`.
    const s = c.accept(sock(listener), null, null);
    if (s == INVALID_SOCKET) return @bitCast(@as(isize, -1));
    const fd: i32 = @intCast(s);
    setNonblock(fd);
    return wrapSock(s);
}

/// Whether this socket is listening. The IOCP backend has to know, because a
/// listener is armed with `AcceptEx` and everything else with a zero-byte
/// receive, and the reactor above does not track which is which.
pub fn isListening(fd: i32) bool {
    var on: c_int = 0;
    var len: c_int = @sizeOf(c_int);
    if (c.getsockopt(sock(fd), ws2.SOL.SOCKET, SO_ACCEPTCONN, &on, &len) != 0) return false;
    return on != 0;
}

const SO_ACCEPTCONN: c_int = 0x0002;

const WSAPOLLFD = extern struct { fd: SOCKET, events: i16, revents: i16 };
const POLLRDNORM: i16 = 0x0100;

/// Wait for one socket to become readable, or for the timeout.
///
/// `WSAPoll` rather than `poll`, and this exists at all because Zig 0.16's
/// `std.posix.pollfd` does not compile for Windows: it aliases a `ws2_32.pollfd`
/// that the types-only ws2_32 module never declares. So `std.posix.poll` is not
/// available here regardless of whether it would work.
///
/// `POLLIN` is spelled `POLLRDNORM`, and unlike `poll` this one is sockets only.
pub fn pollReadable(fd: i32, timeout_ms: i32) bool {
    var p = [_]WSAPOLLFD{.{ .fd = sock(fd), .events = POLLRDNORM, .revents = 0 }};
    return c.WSAPoll(&p, 1, timeout_ms) > 0;
}

// --- the poll seam, for the load generators

/// `poll` over a whole set. Same three names as the other two platforms, but
/// the struct is not the same: the descriptor field is a `SOCKET`, not an
/// `int`. That is why callers go through `pollFd` instead of assigning the
/// descriptor directly.
///
/// `POLLIN` is `POLLRDNORM` here and `POLLOUT` is `POLLWRNORM`.
pub const PollFd = WSAPOLLFD;
pub const poll_in: i16 = POLLRDNORM;
pub const poll_out: i16 = POLLWRNORM;

pub fn pollFd(fd: i32) SOCKET {
    return sock(fd);
}

pub fn pollSet(fds: []PollFd, timeout_ms: i32) usize {
    const rc = c.WSAPoll(fds.ptr, @intCast(fds.len), timeout_ms);
    return if (rc < 0) 0 else @intCast(rc);
}

const POLLWRNORM: i16 = 0x0010;

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

/// Close, with the half-close that makes it mean what `close` means on the
/// other two platforms.
///
/// Closing a socket that still has unreceived data in its receive buffer is an
/// abortive close on Windows: the stack sends RST, and everything sitting in
/// the send buffer is thrown away. That is not a hypothetical. The server
/// answers a connection it could not find a buffer for by writing "503 no
/// buffer" and closing, without ever reading the request, so there is always
/// unread data on exactly the connections whose answer matters most. Measured
/// on this machine, with the request unread: close alone delivered 0 bytes of
/// a 78-byte response, and a `shutdown` first delivered all 78.
///
/// The `shutdown` sends FIN, which flushes what is queued, and the close then
/// tears down what is left. Callers see the POSIX behaviour they were written
/// against, which is the entire job of this file.
///
/// The result is ignored on purpose. A socket that is already gone cannot be
/// shut down, and that is not a reason to skip closing it.
pub fn close(fd: i32) void {
    _ = c.shutdown(sock(fd), SD_SEND);
    _ = c.closesocket(sock(fd));
}

const SD_SEND: c_int = 1;

// ------------------------------------------------------ time and the process

/// Millisecond resolution, which is coarser than the other two platforms
/// offer. `sleepMs` in `sys.zig` loops against the monotonic clock, so the
/// coarseness costs accuracy on a single call and not on the total.
pub fn sleepRelNs(ns: u64) void {
    const ms = ns / 1_000_000;
    c.Sleep(@intCast(@min(ms, std.math.maxInt(u32))));
}

/// The preemption tick, as a timer-queue timer.
///
/// Everywhere else this is `setitimer` and a SIGALRM handler. Windows has no
/// signals, so the nearest honest equivalent is a kernel timer that calls back
/// on a thread of its own, which is what `CreateTimerQueueTimer` is.
///
/// It is not the same thing, and the difference is worth being clear about. A
/// signal interrupts the running thread and the handler executes on it; a timer
/// callback runs *beside* the running thread. So this cannot interrupt a task
/// that is in a tight loop, it can only set the flag the task will read at its
/// next yield point. For asking a well-behaved task to yield, which is what the
/// flag is for, the two are equivalent. For catching a task that reaches no
/// yield point at all, the signal version can at least run its watchdog while
/// the task spins, and so can this, because the callback thread is not the
/// stuck one. What is genuinely lost is nothing, as it turns out: the handler
/// never preempted anybody either, it only ever set a flag and counted.
///
/// `WT_EXECUTEINTIMERTHREAD` runs the callback on the timer thread instead of
/// handing it to the thread pool. That is documented as being for callbacks
/// that finish quickly, which this one does: some atomics and a comparison.
/// It also serialises callbacks for this timer, so the handler-private counters
/// have one writer, same as under a signal handler.
///
/// Zero deletes the timer, which is what `lazy_tick` asks for before a real
/// sleep.
pub var tick_handler: ?*const fn () void = null;

var timer_handle: ?*anyopaque = null;

fn timerCallback(param: ?*anyopaque, fired: u8) callconv(.winapi) void {
    _ = param;
    _ = fired;
    if (tick_handler) |f| f();
}

pub fn armIntervalTimer(ms: u64) void {
    if (timer_handle) |h| {
        // The completion event is the invalid-handle sentinel, which means
        // "wait for any callback in progress to finish". Without it a callback
        // can still be running against state the caller is about to change.
        // Safe here because this is never called from inside the callback.
        _ = c.DeleteTimerQueueTimer(null, h, INVALID_HANDLE_VALUE);
        timer_handle = null;
    }
    if (ms == 0) return;
    var h: ?*anyopaque = null;
    const period: u32 = @intCast(@min(ms, std.math.maxInt(u32)));
    if (c.CreateTimerQueueTimer(&h, null, timerCallback, null, period, period, WT_EXECUTEINTIMERTHREAD) != .FALSE) {
        timer_handle = h;
    }
}

const WT_EXECUTEINTIMERTHREAD: u32 = 0x0000_0020;
const INVALID_HANDLE_VALUE: ?*anyopaque = @ptrFromInt(std.math.maxInt(usize));

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
