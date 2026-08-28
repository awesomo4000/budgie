//! The write path, under the conditions batching created.
//!
//! Appending pipelined answers into one buffer made three code paths ordinary
//! that had been nearly dead, and none of them had a test.
//!
//! A burst larger than the output buffer. `out` holds about 130 answers, so a
//! client sending more than that forces the server to write, refill and write
//! again mid-run. This is where the bug was: carrying pipelined bytes across a
//! keep-alive reset copied them through a 512-byte scratch array while `in` is
//! 16 KB, so a client pipelining more than about fifteen requests wrote past
//! the end of a stack buffer with bytes it chose. A safe build panics. A
//! release build does whatever the stack happens to hold.
//!
//! A partial write. With 256-byte responses a write almost always finished in
//! one call, so `out_sent` accumulating across passes was rarely exercised.
//! With 8 KB going out at once it is the normal case.
//!
//! A peer that stops reading. The socket fills, the write stops short, and the
//! server parks holding a full buffer. That is the whole backpressure story
//! and it ran untested.
//!
//! Two notes on making the last one happen, because neither was obvious.
//!
//! Shrinking the client's receive buffer does almost nothing. Both platforms
//! auto-tune socket buffers into the megabytes once data flows, so filling a
//! loopback connection by volume alone means leaving eight megabytes of
//! response unread. Bounding the SERVER's send buffer works, and that is what
//! `send_buf_bytes` is for.
//!
//! And the client has to send without blocking. A server parked on its own
//! write has stopped reading, so a blocking send against it hangs rather than
//! fails, and a hang in a test reads as nothing at all.
//!
//! Every case checks answers in order and by value, not by count. Each request
//! asks for a different number of work units and the server echoes what it
//! spent, so a reordered, duplicated or dropped answer fails rather than
//! passing on an arithmetic coincidence.

const std = @import("std");
const budgie = @import("budgie");
const server = @import("server");
const hc = @import("httpclient");

const Client = hc.Client;
const check = hc.check;

var bound_port: std.atomic.Value(u16) = .init(0);
var start_failed: std.atomic.Value(bool) = .init(false);

fn serverThread() void {
    const p = server.start(0) catch {
        start_failed.store(true, .release);
        return;
    };
    bound_port.store(p, .release);
    server.runUntil(std.math.maxInt(i64));
}

fn startServer() !u16 {
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

/// Build a burst of `n` requests, each asking for a different unit count so
/// the answers can be told apart. Every request is the same length, so a
/// caller that only got part of the burst out can divide and know how many
/// whole requests the server actually received.
fn buildBurst(buf: []u8, n: usize) []const u8 {
    const w = hc.work_request_bytes;
    for (0..n) |i| _ = hc.workRequest(i % 900, buf[i * w ..][0..w]);
    return buf[0 .. n * w];
}

/// Walk the answers and confirm they carry the units asked for, in order.
/// Returns the number matched, and reports the first mismatch.
fn verifyInOrder(bytes: []const u8, n: usize, label: []const u8) usize {
    var matched: usize = 0;
    var i: usize = 0;
    while (matched < n) {
        const at = std.mem.indexOfPos(u8, bytes, i, "spent") orelse break;
        var it = std.mem.tokenizeAny(u8, bytes[at + 5 ..], " +u");
        const tok = it.next() orelse break;
        const got = std.fmt.parseInt(i64, tok, 10) catch break;
        const want: i64 = @intCast(matched % 900);
        if (got != want) {
            std.debug.print("        {s}: answer {d} said {d}, expected {d}\n", .{ label, matched, got, want });
            return matched;
        }
        matched += 1;
        i = at + 5;
    }
    return matched;
}

/// One response is 63 bytes; size reads generously.
const per_answer = 64;

// ---------------------------------------------------------------- cases

/// More answers than `out` can hold, so the run spans several writes.
///
/// This is the case that found the bug this file was written for. Carrying
/// pipelined bytes across a keep-alive reset went through a 512-byte scratch
/// array while `in` is 16 KB, so a burst of about fifteen requests wrote past
/// the end of a stack buffer with attacker-chosen bytes. Safe builds panicked.
/// A release build would not have.
fn tBurstBeyondBuffer(port: u16, n: usize) !void {
    const c = try Client.connect(port);
    defer c.close();

    const burst = try std.heap.page_allocator.alloc(u8, n * hc.work_request_bytes);
    defer std.heap.page_allocator.free(burst);
    try c.send(buildBurst(burst, n));

    const reply = try std.heap.page_allocator.alloc(u8, n * per_answer + 4096);
    defer std.heap.page_allocator.free(reply);
    const got = c.recvUntil(reply, "units", n, 15000);
    const ok = verifyInOrder(reply[0..got], n, "burst");
    check(ok == n, "pipelined burst larger than the output buffer", .{ .want = n, .in_order = ok, .bytes = got });
}

var mark: u64 = 0;

/// Either shape of backpressure. Which one a platform produces is not a choice
/// the server makes: macOS copies what fits and reports a partial write, Linux
/// refuses the whole thing with EAGAIN. Both mean the same thing here, so a
/// test that insisted on one of them would fail on the other platform for no
/// reason at all.
fn sawBackpressure() bool {
    const st = server.stats();
    return st.partial_writes + st.write_stalls > mark;
}

/// A pipeline deeper than the input buffer holds.
///
/// `in` is 16 KB and a request here is 34 bytes, so somewhere past four
/// hundred a burst stops being a write-path question and becomes an admission
/// one. The two backends answer it differently and both are right: the
/// readiness server reads from the socket only when it has room, so the
/// backlog waits in the kernel and every request is eventually served; the
/// completion server has already been handed the bytes by the time it looks,
/// cannot push back on a shared buffer ring, and refuses with 503.
///
/// What must be true either way is that the client is told. Not silence, not a
/// truncated run of answers, and not an answer to a request it did not send.
fn tPipelineDeeperThanInput(port: u16, n: usize) !void {
    const c = try Client.connect(port);
    defer c.close();

    const burst = try std.heap.page_allocator.alloc(u8, n * hc.work_request_bytes);
    defer std.heap.page_allocator.free(burst);
    try c.send(buildBurst(burst, n));

    const reply = try std.heap.page_allocator.alloc(u8, n * per_answer + 4096);
    defer std.heap.page_allocator.free(reply);
    const got = c.recvUntil(reply, "units", n, 15000);
    const answered = verifyInOrder(reply[0..got], n, "deep pipeline");
    const refused = hc.statusIs(reply[0..got], "503");

    check(got > 0, "a pipeline too deep to buffer is answered, not ignored", .{ .bytes = got });
    check(answered == n or refused, "and the answer is either all of them or a refusal", .{
        .in_order = answered,
        .want = n,
        .status = hc.statusLine(reply[0..got]),
    });
}

/// A peer that stops reading, which is the only way to reach the two write
/// paths batching made ordinary.
///
/// Getting there took a knob. Shrinking the client's receive buffer does
/// nothing: macOS auto-tunes it back up to megabytes as soon as data flows,
/// and the server's own send buffer grows the same way, so filling the
/// connection means leaving eight megabytes of response unread. Bounding the
/// server's send buffer instead (`send_buf_bytes`) reaches EAGAIN in one round
/// trip and is a limit an operator might want anyway.
///
/// The send has to be non-blocking too. Once the server parks on its write it
/// stops reading, and a blocking send against that hangs rather than fails.
fn tPeerStopsReading(port: u16) !void {
    const n = 400;
    // Let the previous case's teardown land first. Its close is asynchronous,
    // and counting it here would read as this case losing a connection.
    _ = hc.waitUntil(buffersBalanced, 3000);
    hc.sleepMs(200);
    const before = server.stats();

    const c = try Client.connect(port);
    defer c.close();
    // Both ends. The server's send buffer bounds what it can hold; this end's
    // receive window bounds what it can hand over. Linux needs the second,
    // because on loopback the send buffer drains as fast as the receiver will
    // take it and a wide window means it never fills.
    c.throttleReceive(2048);
    c.setNonblocking();

    const burst = try std.heap.page_allocator.alloc(u8, n * hc.work_request_bytes);
    defer std.heap.page_allocator.free(burst);
    const bytes = buildBurst(burst, n);

    // Push without reading. The server parks partway through and stops
    // draining, so the last of this may not go out; count what did.
    var sent: usize = 0;
    var waited: usize = 0;
    mark = before.partial_writes + before.write_stalls;
    while (sent < bytes.len and waited < 1000) {
        sent += c.sendSome(bytes[sent..]);
        if (sawBackpressure()) break;
        hc.sleepMs(5);
        waited += 5;
    }
    _ = hc.waitUntil(sawBackpressure, 1000);
    const complete = sent / hc.work_request_bytes;

    const mid = server.stats();
    check(mid.partial_writes + mid.write_stalls > mark, "an unfinished write parks rather than spinning", .{
        .partial = mid.partial_writes - before.partial_writes,
        .stalled = mid.write_stalls - before.write_stalls,
        .sent = complete,
    });
    // The bug this guards against is treating every write error as EAGAIN's
    // opposite. A server that mistook backpressure for a dead peer would show
    // up here as a closed connection, and the drain below would come up short.
    check(mid.ended_peer_gone == before.ended_peer_gone, "backpressure is not mistaken for a dead peer", .{
        .closed = mid.ended_peer_gone - before.ended_peer_gone,
    });
    check(mid.buf_live <= 2, "one stalled connection holds one buffer, not many", .{ .live = mid.buf_live });

    // Now drain. Every request that got all the way in must still be answered,
    // in order, with the units it asked for.
    const reply = try std.heap.page_allocator.alloc(u8, complete * per_answer + 8192);
    defer std.heap.page_allocator.free(reply);
    const got = c.recvUntil(reply, "units", complete, 30000);
    const ok = verifyInOrder(reply[0..got], complete, "stalled peer");
    check(ok == complete and complete > 0, "a peer that stops reading still gets every answer, in order", .{
        .want = complete,
        .in_order = ok,
    });

    // The window opens in pieces as the client reads, so the writes that
    // follow move some bytes and not all. That is `out_sent` accumulating
    // across passes, which nothing exercised before responses got to 8 KB.
    const after = server.stats();
    check(after.partial_writes + after.write_stalls > mark, "a response is written in pieces and reassembled correctly", .{
        .partial = after.partial_writes - before.partial_writes,
        .stalled = after.write_stalls - before.write_stalls,
    });
}

/// A client that keeps asking and never listens, then finally reads.
///
/// A peer that asks for hundreds of answers without reading one must not be
/// hung up on, and must still get them all once it starts reading.
///
/// It is also the shortest route to the write that moves no bytes at all,
/// though only on Linux. There a send with no room returns EAGAIN outright;
/// macOS copies whatever fits and reports a partial write instead, so the
/// zero-byte branch stays unvisited on that platform however hard this leans
/// on it. Hence `sawBackpressure` rather than a check on either counter.
fn tPeerNeverReads(port: u16) !void {
    _ = hc.waitUntil(buffersBalanced, 3000);
    hc.sleepMs(200);
    const before = server.stats();

    const c = try Client.connect(port);
    defer c.close();
    c.throttleReceive(2048);
    c.setNonblocking();

    var rq: [128]u8 = undefined;
    const req = hc.workRequest(1, &rq);
    var asked: usize = 0;
    while (asked < 200) : (asked += 1) {
        if (c.sendSome(req) != req.len) break; // our own send side backed up
        hc.sleepMs(2); // arrive one at a time, so each answer is written alone
    }
    hc.sleepMs(300);

    const mid = server.stats();
    check(mid.ended_peer_gone == before.ended_peer_gone, "a peer that never reads is not hung up on", .{
        .closed = mid.ended_peer_gone - before.ended_peer_gone,
        .asked = asked,
    });

    var reply: [64 * 1024]u8 = undefined;
    const got = c.recvUntil(&reply, "units", asked, 15000);
    const answers = hc.countOf(reply[0..got], "spent");
    check(answers == asked, "the backlog comes out once the peer reads", .{
        .asked = asked,
        .answers = answers,
    });
}

/// Several connections pipelining at once, none of them reading until all have
/// sent. Each holds a buffer while it has work outstanding, which is the case
/// that would exhaust the pool if a parked write leaked one.
fn tManySlowReaders(port: u16) !void {
    const conns = 16;
    const n = 100;
    var clients: [conns]Client = undefined;
    var opened: usize = 0;
    defer for (clients[0..opened]) |c| c.close();

    const burst = try std.heap.page_allocator.alloc(u8, n * hc.work_request_bytes);
    defer std.heap.page_allocator.free(burst);
    const req = buildBurst(burst, n);

    while (opened < conns) : (opened += 1) {
        clients[opened] = Client.connect(port) catch break;
        clients[opened].throttleReceive(4096);
        clients[opened].send(req) catch {};
    }
    hc.sleepMs(500); // everyone waiting at once

    var good: usize = 0;
    const reply = try std.heap.page_allocator.alloc(u8, n * per_answer + 8192);
    defer std.heap.page_allocator.free(reply);
    for (clients[0..opened]) |c| {
        const got = c.recvUntil(reply, "units", n, 20000);
        if (verifyInOrder(reply[0..got], n, "many readers") == n) good += 1;
    }
    check(good == opened and opened == conns, "many pipelining connections all drain correctly", .{
        .opened = opened,
        .complete = good,
    });
}

fn buffersBalanced() bool {
    const st = server.stats();
    return st.buf_acquires == st.buf_releases;
}

fn tBuffersBalance() !void {
    _ = hc.waitUntil(buffersBalanced, 5000);
    const st = server.stats();
    check(st.buf_acquires == st.buf_releases and st.buf_live == 0, "every buffer released after all of it", .{
        .acquires = st.buf_acquires,
        .releases = st.buf_releases,
        .live = st.buf_live,
    });
}

fn tStillHealthy(port: u16) !void {
    const c = try Client.connect(port);
    defer c.close();
    var rq: [128]u8 = undefined;
    try c.send(hc.request("/work/77", &rq));
    var buf: [512]u8 = undefined;
    const got = c.recvUntil(&buf, "units", 1, 5000);
    check(std.mem.indexOf(u8, buf[0..got], "+77") != null, "server healthy after the write-path cases", got);
}

pub fn main() !void {
    hc.ignoreSigpipe();
    // Small enough that a run of answers cannot leave in one write. Without
    // it the backpressure checks below never see the path they exist for.
    server.knobs().send_buf_bytes = 2048;
    // A connection parked on a write it cannot finish is still idle by the
    // server's reckoning, and the default three seconds is short enough that
    // the stall below trips it. Being reaped for that is correct and
    // deadline_test is where it belongs; here it would just mean answers
    // stopping partway with nothing to show why.
    server.knobs().idle_deadline_ms = 15000;
    const port = try startServer();
    std.debug.print("writepath_test: server on 127.0.0.1:{d}\n", .{port});

    // 130 answers is roughly what `out` holds; 16 requests is roughly what the
    // old scratch buffer held. These straddle both.
    try tBurstBeyondBuffer(port, 20);
    try tBurstBeyondBuffer(port, 64);
    try tBurstBeyondBuffer(port, 200);
    try tBurstBeyondBuffer(port, 400);
    // Past here it is the input buffer that decides, not the output one.
    try tPipelineDeeperThanInput(port, 600);
    try tPeerStopsReading(port);
    try tPeerNeverReads(port);
    try tManySlowReaders(port);
    try tStillHealthy(port);
    try tBuffersBalance();

    hc.report();
}
