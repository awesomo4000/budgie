//! End-to-end tests for `app/server.zig`, over real sockets.
//!
//! `echo_test.zig` covers the shape a server has to get right: framing,
//! pipelining, hangups, slot reuse. This one covers what `app/server.zig`
//! adds on top and the example deliberately leaves out. Execution budgets and
//! what happens when a request exceeds one. The cleanup reserve that has to
//! fund an unwind after the budget it would have used is gone. The buffer
//! pool as an admission decision rather than an allocation failure. The
//! supervisor tree. The control surface on the next port up.
//!
//! Where it can, it asserts against the server's own counters as well as
//! against the bytes on the wire. A response proves the request was answered.
//! `acquires == releases` proves nothing leaked while answering it, and that
//! is the assertion most likely to catch the next real bug.
//!
//! One limit worth stating. Signal disposition is per process, and the server
//! shares this one, so `hc.ignoreSigpipe()` here also covers the server. That
//! means this file cannot verify the server's own SIGPIPE handling: removing
//! `sys.ignoreSigpipe()` from `start` leaves all these checks passing. It was
//! verified out of process instead, by running the real binary and sending 400
//! request-then-reset cycles at it. Before the fix that killed it after about
//! three, exit status 141. After, it serves all 400 and keeps going.
//!
//! The server runs on its own thread for the same reason as in `echo_test`:
//! so the test can block on a socket without deadlocking against a loop that
//! is single threaded by design.

const std = @import("std");
const budgie = @import("budgie");
const server = @import("server");
const hc = @import("httpclient");

const Client = hc.Client;
const check = hc.check;

/// `GET /work/N` asks for N units of CPU work, so request cost is a parameter.
fn workReq(units: i64, buf: []u8) []const u8 {
    var target: [64]u8 = undefined;
    const t = std.fmt.bufPrint(&target, "/work/{d}", .{units}) catch @panic("target buffer too small");
    return hc.request(t, buf);
}

/// The server answers with "done, spent N units" or a 503. Long enough to
/// hold either.
const reply_len = 64;

fn spentUnits(bytes: []const u8) ?i64 {
    const at = std.mem.indexOf(u8, bytes, "spent") orelse return null;
    var it = std.mem.tokenizeAny(u8, bytes[at + 5 ..], " +u");
    const tok = it.next() orelse return null;
    return std.fmt.parseInt(i64, tok, 10) catch null;
}

// ---------------------------------------------------------------- cases

fn tWorkUnitsCharged(port: u16) !void {
    // The whole point of the toy protocol: cost is an input, and the server
    // reports what it actually spent.
    const cases = [_]i64{ 0, 1, 10, 100, 500, 900 };
    var bad: usize = 0;
    for (cases) |want| {
        const c = Client.connect(port) catch {
            bad += 1;
            continue;
        };
        defer c.close();
        var rq: [128]u8 = undefined;
        c.send(workReq(want, &rq)) catch {
            bad += 1;
            continue;
        };
        var buf: [512]u8 = undefined;
        const n = c.recvUntil(&buf, "units", 1, 3000);
        const got = spentUnits(buf[0..n]) orelse {
            bad += 1;
            continue;
        };
        if (got != want) bad += 1;
    }
    check(bad == 0, "work units charged exactly as requested", .{ .bad = bad });
}

fn tBudgetExhausted(port: u16) !void {
    // work_budget defaults to 1000. Asking for more must be refused with a
    // 503 rather than served slowly or dropped.
    const c = try Client.connect(port);
    defer c.close();
    var rq: [128]u8 = undefined;
    try c.send(workReq(50_000, &rq));

    var buf: [512]u8 = undefined;
    const n = c.recvUntil(&buf, "503", 1, 5000);
    const got = buf[0..n];
    check(std.mem.indexOf(u8, got, "503") != null and
        std.mem.indexOf(u8, got, "budget") != null, "over-budget request refused with 503", got.len);
}

fn tBudgetRefusalIsClean(port: u16) !void {
    // The cleanup reserve exists so an unwind is funded after the work that
    // overran it. If it were not, the connection would be dropped without the
    // 503 ever being written.
    const before = server.stats();
    const c = try Client.connect(port);
    defer c.close();
    var rq: [128]u8 = undefined;
    try c.send(workReq(50_000, &rq));
    var buf: [512]u8 = undefined;
    _ = c.recvUntil(&buf, "503", 1, 5000);

    // Let the server finish the teardown before reading its counters.
    hc.sleepMs(200);
    const after = server.stats();
    const budget_endings = after.ended_budget - before.ended_budget;
    check(budget_endings >= 1, "budget refusal recorded as its own ending", .{
        .before = before.ended_budget,
        .after = after.ended_budget,
    });
}

fn tSurvivesBudgetStorm(port: u16) !void {
    // Twenty over-budget requests in a row. Each one has to unwind out of its
    // reserve; if any of them leaked a buffer the pool would drain.
    var refused: usize = 0;
    for (0..20) |_| {
        const c = Client.connect(port) catch continue;
        defer c.close();
        var rq: [128]u8 = undefined;
        c.send(workReq(50_000, &rq)) catch continue;
        var buf: [512]u8 = undefined;
        const n = c.recvUntil(&buf, "503", 1, 5000);
        if (std.mem.indexOf(u8, buf[0..n], "503") != null) refused += 1;
    }
    check(refused == 20, "twenty over-budget requests all refused", .{ .refused = refused });
}

fn tKeepAliveAndPipelining(port: u16) !void {
    const c = try Client.connect(port);
    defer c.close();

    var ok = true;
    var rq: [128]u8 = undefined;
    for (0..5) |_| {
        try c.send(workReq(10, &rq));
        var buf: [512]u8 = undefined;
        const n = c.recvUntil(&buf, "units", 1, 3000);
        if (spentUnits(buf[0..n]) != @as(?i64, 10)) ok = false;
    }
    check(ok, "keep-alive, five sequential requests", .{});

    // Now four at once on a fresh connection.
    const p = try Client.connect(port);
    defer p.close();
    var burst: [1024]u8 = undefined;
    var used: usize = 0;
    for (0..4) |_| {
        var one_buf: [128]u8 = undefined;
        const one = workReq(5, &one_buf);
        @memcpy(burst[used..][0..one.len], one);
        used += one.len;
    }
    try p.send(burst[0..used]);
    var buf: [4096]u8 = undefined;
    const n = p.recvUntil(&buf, "units", 4, 4000);
    const got = hc.countOf(buf[0..n], "spent");
    check(got == 4, "pipelined burst of four answered in full", .{ .got = got });
}

fn tControlSurface(port: u16) !void {
    // The control surface listens on port+1 at the top priority class, and is
    // deliberately not an HTTP endpoint: it echoes, so that a surface which
    // can be made to do arbitrary work is not a control surface.
    const c = Client.connect(port + 1) catch {
        check(false, "control surface accepts a connection", .{ .port = port + 1 });
        return;
    };
    defer c.close();
    try c.send("ping\n");
    var buf: [64]u8 = undefined;
    const n = c.recv(&buf, 5, 3000);
    check(n >= 5 and std.mem.startsWith(u8, buf[0..n], "ping"), "control surface echoes", buf[0..n]);
}

fn tControlSurfaceUnderLoad(port: u16) !void {
    // The reason it is top priority: it has to answer while connections are
    // saturating the machine. Start work, then ask.
    var loaders: [8]Client = undefined;
    var opened: usize = 0;
    defer for (loaders[0..opened]) |c| c.close();
    while (opened < loaders.len) : (opened += 1) {
        loaders[opened] = Client.connect(port) catch break;
        var rq: [128]u8 = undefined;
        loaders[opened].send(workReq(900, &rq)) catch {};
    }

    const c = Client.connect(port + 1) catch {
        check(false, "control surface answers under load", .{});
        return;
    };
    defer c.close();
    try c.send("busy\n");
    var buf: [64]u8 = undefined;
    const n = c.recv(&buf, 5, 3000);
    check(n >= 5, "control surface answers while connections are working", n);
}

fn tMalformed(port: u16) !void {
    const cases = [_][]const u8{
        " / HTTP/1.1\r\nHost: x\r\n\r\n", // empty method
        "GET\r\n\r\n", // no space after method
        "\r\n\r\n", // no request line
        "GET /x\r\n\r\n", // no space after target
        "GET nopath HTTP/1.1\r\n\r\n", // target is not a path
    };
    var bad: usize = 0;
    for (cases) |req| {
        const c = Client.connect(port) catch {
            bad += 1;
            continue;
        };
        defer c.close();
        c.send(req) catch {
            bad += 1;
            continue;
        };
        var buf: [512]u8 = undefined;
        const n = c.recv(&buf, reply_len, 1500);
        if (std.mem.indexOf(u8, buf[0..n], "spent") != null) bad += 1;
    }
    check(bad == 0, "malformed requests never answered with work", .{ .bad = bad });
}

fn tOversized(port: u16) !void {
    const c = try Client.connect(port);
    defer c.close();
    var big: [8192]u8 = undefined;
    @memset(&big, 'A');
    try c.send("GET /work/");
    c.send(&big) catch {};
    c.send("\r\n\r\n") catch {};
    var buf: [512]u8 = undefined;
    const n = c.recv(&buf, reply_len, 2000);
    check(std.mem.indexOf(u8, buf[0..n], "spent") == null, "oversized request refused", n);
}

fn tByteAtATime(port: u16) !void {
    const c = try Client.connect(port);
    defer c.close();
    var rq: [128]u8 = undefined;
    const req = workReq(25, &rq);
    for (req) |b| try c.send(&[_]u8{b});
    var buf: [512]u8 = undefined;
    const n = c.recvUntil(&buf, "units", 1, 4000);
    check(spentUnits(buf[0..n]) == @as(?i64, 25), "request delivered one byte at a time", n);
}

fn tEverySplit(port: u16) !void {
    var rq: [128]u8 = undefined;
    const req = workReq(7, &rq);
    var bad: usize = 0;
    var at: usize = 1;
    while (at < req.len) : (at += 1) {
        const c = Client.connect(port) catch {
            bad += 1;
            continue;
        };
        defer c.close();
        c.send(req[0..at]) catch {
            bad += 1;
            continue;
        };
        c.send(req[at..]) catch {
            bad += 1;
            continue;
        };
        var buf: [512]u8 = undefined;
        const n = c.recvUntil(&buf, "units", 1, 3000);
        if (spentUnits(buf[0..n]) != @as(?i64, 7)) bad += 1;
    }
    check(bad == 0, "split at every byte boundary", .{ .offsets = req.len - 1, .bad = bad });
}

fn tHangupsAndVanish(port: u16) !void {
    // Half a request then gone, and connect-then-gone. Both have to reclaim
    // the task and the buffer.
    for (0..15) |_| {
        const c = Client.connect(port) catch continue;
        var rq: [128]u8 = undefined;
        const req = workReq(100, &rq);
        c.send(req[0 .. req.len / 2]) catch {};
        c.close();
    }
    for (0..15) |_| {
        const c = Client.connect(port) catch continue;
        c.close();
    }
    check(true, "hangups mid-request and immediate closes accepted", .{});
}

/// A client that sends a request and then resets the connection.
///
/// Two separate faults lived here, and both were remote: the server's write
/// landed on a torn-down socket and took SIGPIPE, whose default disposition
/// is to terminate, so any peer could kill the process with one reset. Fixing
/// that exposed the second, because with the signal ignored the write returned
/// EPIPE and the code treated every write error as would-block. The task
/// parked on a dead descriptor, kept waking on I/O rather than on its
/// deadline, and was never reclaimed: 397 buffers and 401 tasks held after 400
/// resets.
fn tResetFlood(port: u16) !void {
    const before = server.stats();
    const rounds = 200;
    var sent: usize = 0;
    var rq: [128]u8 = undefined;
    const req = workReq(200, &rq);

    for (0..rounds) |_| {
        const c = Client.connect(port) catch continue;
        c.resetOnClose();
        c.send(req) catch {
            c.close();
            continue;
        };
        c.close(); // RST, while the answer is being written
        sent += 1;
    }

    // Surviving at all is the first assertion. Before the fix the process was
    // gone by roughly the third round.
    check(sent > rounds / 2, "server survived a flood of resets", .{ .sent = sent, .of = rounds });

    hc.sleepMs(500);
    const after = server.stats();
    const accepted = after.accepted - before.accepted;
    const ended = after.endings_total - before.endings_total;
    check(ended >= accepted -| 4, "reset connections were reclaimed, not left parked", .{
        .accepted = accepted,
        .ended = ended,
    });
    check(after.buf_live <= 2, "no buffers left held by reset connections", .{ .live = after.buf_live });
}

fn tConcurrent(port: u16, n_conns: usize) !void {
    var clients: [48]Client = undefined;
    var opened: usize = 0;
    defer for (clients[0..opened]) |c| c.close();
    while (opened < n_conns) : (opened += 1) {
        clients[opened] = Client.connect(port) catch break;
    }
    var rq: [128]u8 = undefined;
    const req = workReq(20, &rq);
    for (clients[0..opened]) |c| try c.send(req);

    var answered: usize = 0;
    for (clients[0..opened]) |c| {
        var buf: [512]u8 = undefined;
        const n = c.recvUntil(&buf, "units", 1, 5000);
        if (spentUnits(buf[0..n]) == @as(?i64, 20)) answered += 1;
    }
    check(answered == opened and opened == n_conns, "many connections served at once", .{ .opened = opened, .answered = answered });
}

fn tSlotReuse(port: u16, rounds: usize) !void {
    var ok = true;
    for (0..rounds) |_| {
        var clients: [12]Client = undefined;
        var opened: usize = 0;
        while (opened < clients.len) : (opened += 1) {
            clients[opened] = Client.connect(port) catch break;
        }
        var rq: [128]u8 = undefined;
        const req = workReq(15, &rq);
        for (clients[0..opened]) |c| c.send(req) catch {
            ok = false;
        };
        for (clients[0..opened]) |c| {
            var buf: [512]u8 = undefined;
            const n = c.recvUntil(&buf, "units", 1, 5000);
            if (spentUnits(buf[0..n]) != @as(?i64, 15)) ok = false;
        }
        for (clients[0..opened]) |c| c.close();
        if (opened != clients.len) ok = false;
    }
    check(ok, "task and buffer slots reused across rounds", .{ .rounds = rounds });
}

fn buffersBalanced() bool {
    const st = server.stats();
    return st.buf_acquires == st.buf_releases;
}

fn tBuffersBalance() !void {
    // The assertion most likely to catch the next real bug. Every buffer
    // handed out across every case above came back.
    _ = hc.waitUntil(buffersBalanced, 3000);
    const st = server.stats();
    check(st.buf_acquires == st.buf_releases and st.buf_live == 0, "every buffer acquired was released", .{
        .acquires = st.buf_acquires,
        .releases = st.buf_releases,
        .live = st.buf_live,
    });
}

fn tEndingsAccountForConnections() !void {
    // Every connection that was accepted ended somewhere, and the reasons are
    // recorded rather than lost.
    const st = server.stats();
    check(st.endings_total > 0 and st.endings_total <= st.accepted, "every ending accounted for", .{
        .accepted = st.accepted,
        .endings_total = st.endings_total,
    });
}

fn tStillHealthy(port: u16) !void {
    const c = try Client.connect(port);
    defer c.close();
    var rq: [128]u8 = undefined;
    try c.send(workReq(42, &rq));
    var buf: [512]u8 = undefined;
    const n = c.recvUntil(&buf, "units", 1, 3000);
    check(spentUnits(buf[0..n]) == @as(?i64, 42), "server healthy after every case above", n);
}

// ---------------------------------------------------------------- driver

/// The port the server bound, published once it is listening.
var bound_port: std.atomic.Value(u16) = .init(0);
var start_failed: std.atomic.Value(bool) = .init(false);

/// `start` runs here rather than on the main thread, and that placement is
/// load-bearing for the io_uring build. It sets IORING_SETUP_SINGLE_ISSUER,
/// which requires every submission to come from the task that created the
/// ring. Creating it on one thread and submitting from another leaves the
/// server accepting connections and answering none of them: every check that
/// waits for a response fails, and only the checks that pass by receiving
/// nothing still pass. The epoll build does not care, which is exactly why
/// this is worth stating.
fn serverThread() void {
    const p = server.start(0) catch {
        start_failed.store(true, .release);
        return;
    };
    bound_port.store(p, .release);
    server.runUntil(std.math.maxInt(i64));
}

/// Retries, because the control surface binds the serving port plus one and
/// that can already be taken. `start` now reports that rather than silently
/// producing a server with no control surface, so the honest response is to
/// ask for a different ephemeral port.
fn startServer() !u16 {
    var attempt: usize = 0;
    while (attempt < 8) : (attempt += 1) {
        bound_port.store(0, .release);
        start_failed.store(false, .release);
        if (startOnce()) |p| return p else |_| {}
    }
    return error.ServerNeverStarted;
}

fn startOnce() !u16 {
    var thread = try std.Thread.spawn(.{}, serverThread, .{});
    thread.detach();
    var waited: usize = 0;
    while (bound_port.load(.acquire) == 0) {
        if (start_failed.load(.acquire)) return error.ServerStartFailed;
        hc.sleepMs(10);
        waited += 1;
        if (waited > 500) return error.ServerNeverStarted;
    }
    return bound_port.load(.acquire);
}

pub fn main() !void {
    hc.ignoreSigpipe();
    const port = try startServer();
    std.debug.print("server_test: server on 127.0.0.1:{d}, control on {d}\n", .{ port, port + 1 });

    try tWorkUnitsCharged(port);
    try tBudgetExhausted(port);
    try tBudgetRefusalIsClean(port);
    try tSurvivesBudgetStorm(port);
    try tKeepAliveAndPipelining(port);
    try tByteAtATime(port);
    try tEverySplit(port);
    try tMalformed(port);
    try tOversized(port);
    try tControlSurface(port);
    try tControlSurfaceUnderLoad(port);
    try tHangupsAndVanish(port);
    try tResetFlood(port);
    try tConcurrent(port, 32);
    try tSlotReuse(port, 4);
    try tStillHealthy(port);
    try tBuffersBalance();
    try tEndingsAccountForConnections();

    hc.checkInvariants(port, "after every case above");
    hc.report();
}
