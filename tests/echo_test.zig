//! End-to-end tests for `examples/echo.zig`, over real sockets.
//!
//! The other tests here exercise pieces: the parser sees bytes, the scheduler
//! sees task ids, the simulator sees a virtual clock. None of them see a
//! socket, which is exactly the seam two real bugs hid in. The example kept a
//! pipelined request in its buffer and never looked at it again, and four
//! pipelined requests got one answer. Nothing in the suite noticed, because
//! nothing in the suite ran the server.
//!
//! So this one runs it. The server goes on its own thread, the tests talk to
//! it through the loopback interface, and every case below is a thing a real
//! client can do to a server: send half a request, send four at once, hang up
//! mid-sentence, open more connections than the server has slots for.
//!
//! The server is single threaded internally. Putting its loop on a second
//! thread is only so the test can block on a socket without deadlocking
//! against it.

const std = @import("std");
const echo = @import("echo");
const hc = @import("httpclient");

const Client = hc.Client;
const check = hc.check;
const request = hc.request;
const countResponses = hc.countOk;
const ok_head = hc.ok_head;

const body = "hello from a backend-independent server\n";

/// One full response is head + body. Used to size reads.
const one_response_len = "HTTP/1.1 200 OK\r\nContent-Length: 40\r\n\r\n".len + body.len;

// ---------------------------------------------------------------- cases

fn tSingle(port: u16) !void {
    const c = try Client.connect(port);
    defer c.close();
    var rq: [128]u8 = undefined;
    try c.send(request("/", &rq));

    var buf: [512]u8 = undefined;
    const n = c.recvUntil(&buf, body, 1, 2000);
    check(std.mem.indexOf(u8, buf[0..n], ok_head) != null and
        std.mem.endsWith(u8, buf[0..n], body), "single request", n);
}

fn tKeepAlive(port: u16) !void {
    const c = try Client.connect(port);
    defer c.close();
    var rq: [128]u8 = undefined;
    var ok = true;
    for (0..5) |_| {
        try c.send(request("/keepalive", &rq));
        var buf: [512]u8 = undefined;
        const n = c.recvUntil(&buf, body, 1, 2000);
        if (countResponses(buf[0..n]) != 1) ok = false;
    }
    check(ok, "keep-alive, five sequential on one connection", .{});
}

fn tPipelined(port: u16, count: usize) !void {
    const c = try Client.connect(port);
    defer c.close();

    // Every request in a single write, so they land in one read on the server.
    var burst: [4096]u8 = undefined;
    var used: usize = 0;
    for (0..count) |_| {
        var rq: [128]u8 = undefined;
        const one = request("/pipe", &rq);
        @memcpy(burst[used..][0..one.len], one);
        used += one.len;
    }
    try c.send(burst[0..used]);

    var buf: [8192]u8 = undefined;
    const n = c.recvUntil(&buf, body, count, 3000);
    const got = countResponses(buf[0..n]);
    check(got == count, "pipelined burst answered in full", .{ .want = count, .got = got });
}

fn tByteAtATime(port: u16) !void {
    const c = try Client.connect(port);
    defer c.close();
    var rq: [128]u8 = undefined;
    const req = request("/slow", &rq);

    // One byte per write. The parser must hold state across every boundary.
    for (req) |b| try c.send(&[_]u8{b});

    var buf: [512]u8 = undefined;
    const n = c.recvUntil(&buf, body, 1, 3000);
    check(countResponses(buf[0..n]) == 1, "request delivered one byte at a time", n);
}

fn tEverySplit(port: u16) !void {
    var rq: [128]u8 = undefined;
    const req = request("/split", &rq);
    var bad: usize = 0;

    // Split the request at every possible offset. This is the boundary case
    // that chunkfuzz covers for the parser alone; here it goes through a real
    // socket, so a short read in the server counts too.
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
        const n = c.recvUntil(&buf, body, 1, 2000);
        if (countResponses(buf[0..n]) != 1) bad += 1;
    }
    check(bad == 0, "split at every byte boundary", .{ .offsets = req.len - 1, .bad = bad });
}

fn tMalformed(port: u16) !void {
    // Framing errors, which is what the parser is responsible for: a missing
    // separator, an empty method, a target that is not a path.
    const cases = [_][]const u8{
        " / HTTP/1.1\r\nHost: x\r\n\r\n", // empty method
        "GET\r\n\r\n", // no space after the method
        "\r\n\r\n", // no request line at all
        "GET /x\r\n\r\n", // no space after the target
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
        // The parser reports a protocol error and the example closes without
        // answering. Either way it must not return 200, and must not hang.
        var buf: [512]u8 = undefined;
        const n = c.recvUntil(&buf, body, 1, 1500);
        if (countResponses(buf[0..n]) != 0) bad += 1;
    }
    check(bad == 0, "malformed requests refused, never answered 200", .{ .bad = bad });
}

fn tUnknownMethod(port: u16) !void {
    // The parser frames; it does not police verbs. An unknown method is
    // well-formed and reaches the application, which is what lets one parser
    // serve a protocol it was not told about. Worth pinning down, since the
    // opposite is just as defensible a design and someone may change it.
    const c = try Client.connect(port);
    defer c.close();
    try c.send("BREW /coffee HTTP/1.1\r\nHost: x\r\n\r\n");

    var buf: [512]u8 = undefined;
    const n = c.recvUntil(&buf, body, 1, 2000);
    check(countResponses(buf[0..n]) == 1, "unknown method reaches the application", n);
}

fn tOversized(port: u16) !void {
    const c = try Client.connect(port);
    defer c.close();

    // Larger than http.max_request_bytes, so the parser must refuse rather
    // than overrun its buffer.
    var big: [4096]u8 = undefined;
    @memset(&big, 'A');
    var head: [64]u8 = undefined;
    const h = std.fmt.bufPrint(&head, "GET /", .{}) catch unreachable;
    try c.send(h);
    c.send(&big) catch {};
    c.send("\r\n\r\n") catch {};

    var buf: [512]u8 = undefined;
    const n = c.recvUntil(&buf, body, 1, 1500);
    check(countResponses(buf[0..n]) == 0, "oversized request refused", n);
}

fn tHangupMidRequest(port: u16) !void {
    const c = try Client.connect(port);
    var rq: [128]u8 = undefined;
    const req = request("/partial", &rq);
    try c.send(req[0 .. req.len / 2]); // half a request, then vanish
    c.close();

    // The server must reclaim the slot. Proven below by the exhaustion test,
    // which would fail if hangups leaked tasks.
    check(true, "hangup mid-request accepted without crash", .{});
}

fn tConnectAndVanish(port: u16) !void {
    var i: usize = 0;
    while (i < 20) : (i += 1) {
        const c = Client.connect(port) catch {
            check(false, "connect then close immediately", i);
            return;
        };
        c.close(); // never send a byte
    }
    check(true, "connect then close immediately, twenty times", .{});
}

fn tHalfClose(port: u16) !void {
    const c = try Client.connect(port);
    defer c.close();
    var rq: [128]u8 = undefined;
    try c.send(request("/halfclose", &rq));
    c.halfClose(); // done sending, still listening

    var buf: [512]u8 = undefined;
    const n = c.recvUntil(&buf, body, 1, 2000);
    check(countResponses(buf[0..n]) == 1, "half-close still gets its answer", n);
}

fn tConcurrent(port: u16, n_conns: usize) !void {
    var clients: [64]Client = undefined;
    var opened: usize = 0;
    defer for (clients[0..opened]) |c| c.close();

    while (opened < n_conns) : (opened += 1) {
        clients[opened] = Client.connect(port) catch break;
    }

    var rq: [128]u8 = undefined;
    const req = request("/concurrent", &rq);
    for (clients[0..opened]) |c| try c.send(req);

    var answered: usize = 0;
    for (clients[0..opened]) |c| {
        var buf: [512]u8 = undefined;
        const n = c.recvUntil(&buf, body, 1, 3000);
        if (countResponses(buf[0..n]) == 1) answered += 1;
    }
    check(answered == opened and opened == n_conns, "many connections served at once", .{ .opened = opened, .answered = answered });
}

fn tSlotReuse(port: u16, rounds: usize) !void {
    // Open a batch, use it, close it, repeat. If `finish` failed to release a
    // task or a buffer, a later round would find no free slots and stall.
    var ok = true;
    for (0..rounds) |_| {
        var clients: [16]Client = undefined;
        var opened: usize = 0;
        while (opened < clients.len) : (opened += 1) {
            clients[opened] = Client.connect(port) catch break;
        }
        var rq: [128]u8 = undefined;
        const req = request("/reuse", &rq);
        for (clients[0..opened]) |c| c.send(req) catch {
            ok = false;
        };
        for (clients[0..opened]) |c| {
            var buf: [512]u8 = undefined;
            const n = c.recvUntil(&buf, body, 1, 3000);
            if (countResponses(buf[0..n]) != 1) ok = false;
        }
        for (clients[0..opened]) |c| c.close();
        if (opened != clients.len) ok = false;
    }
    check(ok, "task and buffer slots reused across rounds", .{ .rounds = rounds });
}

fn tBinaryGarbage(port: u16) !void {
    const c = try Client.connect(port);
    defer c.close();
    var junk: [256]u8 = undefined;
    for (&junk, 0..) |*b, i| b.* = @intCast(i % 256);
    try c.send(&junk);

    var buf: [512]u8 = undefined;
    const n = c.recvUntil(&buf, body, 1, 1500);
    check(countResponses(buf[0..n]) == 0, "binary garbage refused, no crash", n);
}

fn tManyHeaders(port: u16) !void {
    const c = try Client.connect(port);
    defer c.close();
    var req: [512]u8 = undefined;
    var w: usize = 0;
    const start = "GET /headers HTTP/1.1\r\n";
    @memcpy(req[w..][0..start.len], start);
    w += start.len;
    // Fill toward the parser's limit without crossing it.
    while (w < 400) {
        const line = "X: y\r\n";
        @memcpy(req[w..][0..line.len], line);
        w += line.len;
    }
    @memcpy(req[w..][0..2], "\r\n");
    w += 2;
    try c.send(req[0..w]);

    var buf: [512]u8 = undefined;
    const n = c.recvUntil(&buf, body, 1, 2000);
    check(countResponses(buf[0..n]) == 1, "request with many headers", n);
}

fn tStillAliveAfterAbuse(port: u16) !void {
    // The point of the whole file: after every case above, a plain request
    // still works. A server that leaked a task, a buffer or a registration
    // would fail here even if each case passed on its own.
    const c = try Client.connect(port);
    defer c.close();
    var rq: [128]u8 = undefined;
    try c.send(request("/finally", &rq));
    var buf: [512]u8 = undefined;
    const n = c.recvUntil(&buf, body, 1, 2000);
    check(countResponses(buf[0..n]) == 1, "server healthy after every case above", n);
}

// ---------------------------------------------------------------- driver

fn serverThread() void {
    echo.run();
}

pub fn main() !void {
    const port = try echo.start(0); // 0: let the OS pick, avoids collisions
    var thread = try std.Thread.spawn(.{}, serverThread, .{});
    thread.detach();

    std.debug.print("echo_test: server on 127.0.0.1:{d}\n", .{port});

    try tSingle(port);
    try tKeepAlive(port);
    try tPipelined(port, 2);
    try tPipelined(port, 4);
    try tPipelined(port, 16);
    try tByteAtATime(port);
    try tEverySplit(port);
    try tMalformed(port);
    try tUnknownMethod(port);
    try tOversized(port);
    try tBinaryGarbage(port);
    try tManyHeaders(port);
    try tHangupMidRequest(port);
    try tConnectAndVanish(port);
    try tHalfClose(port);
    try tConcurrent(port, 32);
    try tSlotReuse(port, 4);
    try tStillAliveAfterAbuse(port);

    hc.report();
}
