//! A small server, written against the kernel rather than against an I/O API.
//!
//! Build it on Linux and the reactor underneath is epoll; build it on macOS
//! and it is kqueue. Nothing below mentions either, and nothing below changes
//! when you switch. That is the property the whole design is arranged around.
//!
//! It answers every request with the same fixed body, so that what is left is
//! only the shape: a scheduler that hands you a task, a reactor that says when
//! an fd is ready, and a parser that turns bytes into events. Roughly 120
//! lines, and none of them are about the platform.

const std = @import("std");
const budgie = @import("budgie");

const sched = budgie.sched;
const http = budgie.http;
const sys = budgie.sys;
const Reactor = budgie.reactor.Reactor;
const TaskId = sched.TaskId;

/// Task 0 is the listener; 1..max_conns are connections. Task ids double as
/// the index into `conns`, which is what keeps this table lookup-free.
const listener_task: TaskId = 0;
const max_conns = 64;

const body = "hello from a backend-independent server\n";

const Conn = struct {
    fd: i32 = -1,
    parser: http.Parser = .{},
    in: [http.max_request_bytes]u8 = undefined,
    in_len: usize = 0,
    out: [128]u8 = undefined,
    out_len: usize = 0,
    out_sent: usize = 0,
    writing: bool = false,
};

var conns: [max_conns + 1]Conn = @splat(.{});

var s: sched.Sched = .{};
var r: Reactor = .{};
var listener: i32 = -1;

pub fn main() !void {
    const port = try start(8080);

    // The only line in this file that knows a backend exists, and it only
    // knows in order to say so.
    const backend = if (@import("builtin").os.tag == .linux) "epoll" else "kqueue";
    std.debug.print("listening on 127.0.0.1:{d} via {s}\n", .{ port, backend });

    run();
}

/// Bind, and make the listener the first task. Returns the port actually
/// bound, so passing 0 lets the operating system choose one. Split out from
/// `run` so a test can start a server and then talk to it.
pub fn start(port: u16) !u16 {
    sys.ignoreSigpipe();
    listener = try listen(port);
    try r.init();

    s.live[listener_task] = true;
    s.setPrio(listener_task, 1); // accept ahead of serving
    r.watch(listener_task, listener, .read);

    var addr = sys.SockAddrIn{ .port = 0, .addr = 0 };
    if (sys.sysErr(sys.getsockname(listener, &addr))) return error.GetSockNameFailed;
    return sys.netToHostPort(addr.port);
}

/// The loop, which is the whole contract.
pub fn run() noreturn {
    while (true) {
        // 1. Run everything the scheduler says is runnable.
        while (s.popRunnable()) |t| step(t);
        // 2. Block until an fd is ready or a deadline comes due.
        _ = r.wait(&s, 1000);
        // 3. Fire expired timers.
        s.expire(nowMs());
    }
}

fn nowMs() i64 {
    return @divTrunc(budgie.clock.monotonicNs(), 1_000_000);
}

fn step(t: TaskId) void {
    if (t == listener_task) return accept();

    const c = &conns[t];
    if (c.writing) return write(t, c);
    if (drain(t, c)) return; // a pipelined request may already be buffered

    const n = r.read(t, c.fd, c.in[c.in_len..]);
    if (n < 0) {
        // EAGAIN means park and wait. EPIPE or ECONNRESET means the peer is
        // gone, and parking on a dead descriptor holds the task forever.
        if (!sys.wouldBlock(@bitCast(n))) return finish(t, c);
        return r.watch(t, c.fd, .read);
    }
    if (n == 0) return finish(t, c); // peer hung up
    c.in_len += @intCast(n);

    if (drain(t, c)) return;
    r.watch(t, c.fd, .read); // parser wants more bytes
}

/// Feed whatever is buffered to the parser, keeping any remainder. Returns
/// true if the connection changed phase and this turn is over.
///
/// This runs before the read, not only after it. A pipelined client sends the
/// next request without waiting for the previous answer, so `c.in` can already
/// hold a complete request that no amount of further reading will announce.
fn drain(t: TaskId, c: *Conn) bool {
    if (c.in_len == 0) return false;

    const used = c.parser.feed(c.in[0..c.in_len]);
    std.mem.copyForwards(u8, c.in[0 .. c.in_len - used], c.in[used..c.in_len]);
    c.in_len -= used;

    switch (c.parser.poll()) {
        .need_input => return false,
        .protocol_error => {
            finish(t, c);
            return true;
        },
        .request => |req| {
            respond(t, c, req);
            return true;
        },
    }
}

/// Format the answer and hand the task back to the scheduler. No I/O happens
/// here, so the task is made runnable rather than parked: it has work to do
/// and needs a turn, not a wakeup.
fn respond(t: TaskId, c: *Conn, req: http.Request) void {
    _ = req; // a real server would look at req.target here
    c.out_len = (std.fmt.bufPrint(
        &c.out,
        "HTTP/1.1 200 OK\r\nContent-Length: {d}\r\n\r\n{s}",
        .{ body.len, body },
    ) catch return finish(t, c)).len;
    c.out_sent = 0;
    c.writing = true;
    c.parser.reset();
    s.makeRunnable(t, .spawn);
}

fn write(t: TaskId, c: *Conn) void {
    const rc = sys.write(c.fd, c.out[c.out_sent..].ptr, c.out_len - c.out_sent);
    if (sys.sysErr(rc)) {
        if (!sys.wouldBlock(rc)) return finish(t, c);
        return r.watch(t, c.fd, .write);
    }
    c.out_sent += rc;
    if (c.out_sent < c.out_len) return r.watch(t, c.fd, .write);
    c.writing = false;

    // A pipelined client sends the next request without waiting for this
    // answer, so `step` may already have left one sitting in `c.in`. Parking
    // on `.read` here would wait for bytes that have already arrived, and the
    // connection would stall until the peer sent something else or gave up.
    if (c.in_len > 0) return s.makeRunnable(t, .spawn);

    r.watch(t, c.fd, .read); // keep-alive: wait for the next request
}

fn accept() void {
    while (true) {
        const rc = sys.acceptNonblock(listener);
        if (sys.sysErr(rc)) break;
        const fd: i32 = @intCast(rc);
        const t = freeTask() orelse {
            sys.close(fd);
            break;
        };
        conns[t] = .{ .fd = fd };
        s.live[t] = true;
        s.setPrio(t, 2);
        s.admit(t, 1000, 50); // execution budget, and a reserve for cleanup
        r.open(t);
        r.watch(t, fd, .read);
    }
    r.watch(listener_task, listener, .read); // rearm; oneshot disarmed it
}

fn finish(t: TaskId, c: *Conn) void {
    r.unwatch(t);
    r.close(t);
    s.release(t);
    sys.close(c.fd);
    c.fd = -1;
    s.live[t] = false;
}

fn freeTask() ?TaskId {
    var t: TaskId = 1;
    while (t <= max_conns) : (t += 1) {
        if (conns[t].fd < 0) return t;
    }
    return null;
}

fn listen(port: u16) !i32 {
    const rc = sys.tcpSocketNonblock();
    if (sys.sysErr(rc)) return error.SocketFailed;
    const fd: i32 = @intCast(rc);
    sys.setReuseAddr(fd);
    var addr = sys.SockAddrIn{ .port = sys.hostToNetPort(port), .addr = sys.loopback };
    if (sys.sysErr(sys.bind(fd, &addr))) return error.BindFailed;
    if (sys.sysErr(sys.listen(fd, 128))) return error.ListenFailed;
    return fd;
}
