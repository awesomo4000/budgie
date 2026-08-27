//! The deadline path in `app/server.zig`, over real sockets.
//!
//! Its own binary because `idle_deadline_ms` is fixed at startup and the
//! default is three seconds. Set to a fraction of that, the same behaviour is
//! observable in a fraction of the time.
//!
//! This is the one refusal the other socket tests never provoke. A connection
//! that opens and then says nothing is not an error anyone reports: the peer
//! is still there, the socket is fine, and nothing fails. It just stops. The
//! deadline is what turns that into an answer, which makes it exactly the kind
//! of path that can rot without anyone noticing.
//!
//! A note on the sleep between requests, because it cost a long investigation.
//! It has to be measured against the clock, not against however many times a
//! signal interrupted the attempt. `std.posix.poll` retries EINTR with the
//! full timeout again, and this server raises SIGALRM on a 20ms tick, so a
//! 375ms sleep built on it measured 2311ms. The client then sent its next
//! request after the deadline had passed, the server answered 408 exactly as
//! it should, and the test called that a server bug. `sys.sleepMs` loops
//! against an absolute deadline for that reason.
//!
//! Worth being precise about what a 408 means here. It is not the request
//! timing out on the server's side. It is the server declining to hold a task,
//! a slot and a descriptor open indefinitely for a peer that has gone quiet.

const std = @import("std");
const server = @import("server");
const hc = @import("httpclient");

const Client = hc.Client;
const check = hc.check;

/// Short enough to keep the test quick, long enough that a loaded machine does
/// not trip it early. 400ms was not: on a two-core box already running the
/// rest of the suite, a round trip plus the spacing below sometimes exceeded
/// it, and the deadline fired on a connection that was genuinely active.
const deadline_ms = 1500;

/// Generous multiple of the deadline, so a slow box fails the assertion for a
/// real reason rather than for being slow.
const wait_ms = 4000;

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

/// Connect and say nothing at all.
fn tSilentConnection(port: u16) !void {
    const c = try Client.connect(port);
    defer c.close();

    var buf: [512]u8 = undefined;
    const n = c.recvUntil(&buf, "\r\n\r\n", 1, wait_ms);
    const got = buf[0..n];
    check(hc.statusIs(got, "408"), "silent connection answered 408", .{
        .bytes = n,
        .status = hc.statusLine(got),
    });
}

/// Connect, start a request, then stop mid-sentence. The parser is holding
/// state and waiting for more, which is the case a deadline exists for.
fn tHalfRequest(port: u16) !void {
    const c = try Client.connect(port);
    defer c.close();
    try c.send("GET /work/10 HTTP/1.1\r\nHost: x\r\n"); // no terminating blank line

    var buf: [512]u8 = undefined;
    const n = c.recvUntil(&buf, "\r\n\r\n", 1, wait_ms);
    const got = buf[0..n];
    check(hc.statusIs(got, "408"), "half a request answered 408", .{
        .bytes = n,
        .status = hc.statusLine(got),
    });
}

/// A connection that keeps asking must not be cut off. The deadline is re-armed
/// per request, and this is the check that says so.
fn tActiveConnectionSurvives(port: u16) !void {
    const c = try Client.connect(port);
    defer c.close();

    var ok = true;
    var rq: [128]u8 = undefined;
    const req = hc.request("/work/5", &rq);
    // Several rounds, each spaced past half the deadline. Cumulative time is
    // well beyond it, so anything that armed the deadline once and forgot to
    // re-arm would cut this connection off partway through.
    var round: usize = 0;
    var failed_at: usize = 999;
    var reason: []const u8 = "none";
    for (0..5) |i| {
        round = i;
        // A cut-off connection makes this fail to send. That is the result
        // being tested, so record it rather than letting the error abort the
        // run and take the remaining checks with it.
        c.send(req) catch {
            ok = false;
            failed_at = i;
            reason = "send failed";
            break;
        };
        var buf: [512]u8 = undefined;
        const n = c.recvUntil(&buf, "units", 1, wait_ms);
        if (std.mem.indexOf(u8, buf[0..n], "spent") == null) {
            ok = false;
            if (failed_at == 999) {
                failed_at = i;
                reason = if (n == 0) "no reply" else "unexpected reply";
            }
        }
        hc.sleepMs(deadline_ms / 4); // comfortably inside the deadline, repeatedly
    }
    check(ok, "an active connection is never cut off by the deadline", .{
        .round = failed_at,
        .why = reason,
        .rounds = round + 1,
    });
}

fn tCountedAsDeadline(before: server.Stats) !void {
    hc.sleepMs(300); // let the teardowns settle
    const after = server.stats();
    const fired = after.ended_deadline - before.ended_deadline;
    check(fired >= 2, "deadline recorded as its own ending", .{
        .fired = fired,
        .total = after.ended_deadline,
    });
    // A 408 that leaked its buffer would still look right on the wire.
    check(after.buf_acquires == after.buf_releases, "buffers balanced across deadline unwinds", .{
        .acquires = after.buf_acquires,
        .releases = after.buf_releases,
    });
}

fn tStillHealthy(port: u16) !void {
    const c = try Client.connect(port);
    defer c.close();
    var rq: [128]u8 = undefined;
    try c.send(hc.request("/work/21", &rq));
    var buf: [512]u8 = undefined;
    const n = c.recvUntil(&buf, "units", 1, wait_ms);
    check(std.mem.indexOf(u8, buf[0..n], "spent") != null, "server healthy after the deadlines", n);
}

pub fn main() !void {
    hc.ignoreSigpipe();
    server.knobs().idle_deadline_ms = deadline_ms;
    const port = try startServer();
    std.debug.print("deadline_test: server on 127.0.0.1:{d}, idle_deadline_ms={d}\n", .{ port, deadline_ms });

    const before = server.stats();
    try tSilentConnection(port);
    try tHalfRequest(port);
    try tActiveConnectionSurvives(port);
    try tCountedAsDeadline(before);
    try tStillHealthy(port);

    hc.report();
}
