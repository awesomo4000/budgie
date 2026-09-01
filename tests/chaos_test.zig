//! Everything nasty at once, in a random order, against one server.
//!
//! Every other socket test does one pathological thing in isolation: resets in
//! one file, pool exhaustion in another, slow readers in a third. Each was
//! written after a bug, and each guards the shape of the bug it was written
//! for. The two worst bugs this project has had, the SIGPIPE reset flood and
//! the silent `no_buffer` closes, both lived in the space between those tests,
//! and both were found by hand rather than by anything looking for them.
//!
//! So this looks. Thirty-two clients, each step picking a random one and a
//! random thing to do to it: connect, send half a request, pipeline sixty,
//! send garbage, read, go silent, reset, half-close, shed through the control
//! surface. The pool is deliberately too small and the deadline deliberately
//! short, so exhaustion and expiry happen constantly rather than as set pieces.
//!
//! What it asserts, which is deliberately not "the right answers came back",
//! because under this treatment there is no right answer to most of it:
//!
//!   - The server never contradicts itself. `invariants` is asked over the
//!     control surface every so often, which runs the check on the server's
//!     own thread where its state is whole.
//!   - Every response that arrives is a well-formed one of the four statuses
//!     this server can produce. Not silence, not a torn header, not two
//!     answers spliced together.
//!   - It is still serving correctly at the end, and everything balances once
//!     the dust settles.
//!
//! Seeded, and the seed is printed. `zig build chaos -- 12345` replays a
//! failure exactly.
//!
//! The same limit `server_test.zig` states applies here and is worth repeating
//! because this file looks like it should cover it. Signal disposition is per
//! process and the server shares this one, so the `hc.ignoreSigpipe()` the
//! client needs in order to reset anything also covers the server. Removing
//! `sys.ignoreSigpipe()` from `start` leaves every check here passing, on every
//! seed. I confirmed that by planting the regression and watching ten seeds go
//! green before working out why. Verifying it needs the server out of process.

const std = @import("std");
const server = @import("server");
const hc = @import("httpclient");

const Client = hc.Client;
const check = hc.check;

const n_slots = 32;
var steps: usize = 4000;
/// Small enough that acquiring is a real contest, so the 503 admission path
/// runs constantly instead of never.
var io_bufs: i64 = 8;
/// Short enough that connections left alone expire while others are still
/// being driven, so the deadline path interleaves with everything else.
///
/// It was 1200ms and produced exactly zero timeouts on ten seeds, because four
/// thousand steps with no sleeps finish long before that. A test that claims
/// to exercise a path and never does is worse than one that does not claim it,
/// so the number is now small enough to be true.
var deadline_ms: i64 = 60;

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

fn startServer() !u16 {
    var attempt: usize = 0;
    while (attempt < 8) : (attempt += 1) {
        bound_port.store(0, .release);
        start_failed.store(false, .release);
        if (startOnce()) |p| return p else |_| {}
    }
    return error.ServerNeverStarted;
}

const Slot = struct {
    c: ?Client = null,
    /// Requests sent whose answers have not been read. Only used to decide
    /// whether reading is worth trying; nothing is asserted about the count,
    /// because a connection may legitimately be refused mid-pipeline.
    owed: usize = 0,
};

var slots: [n_slots]Slot = @splat(.{});

/// What arrived on the wire, across every connection, for the well-formedness
/// check at the end.
var responses: usize = 0;
var malformed: usize = 0;
var statuses = [_]usize{0} ** 4; // 200, 400, 408, 503
var other_status: usize = 0;
var checks_failed = false;

/// Count what came back and check every response is one this server can
/// actually produce. A torn header or two answers spliced together shows up
/// here and nowhere else.
fn tally(bytes: []const u8) void {
    var i: usize = 0;
    while (std.mem.indexOfPos(u8, bytes, i, "HTTP/1.1 ")) |at| {
        i = at + 9;
        responses += 1;
        if (at + 12 > bytes.len) continue; // truncated by our own read, not the server
        const code = bytes[at + 9 .. at + 12];
        if (std.mem.eql(u8, code, "200")) statuses[0] += 1
        else if (std.mem.eql(u8, code, "400")) statuses[1] += 1
        else if (std.mem.eql(u8, code, "408")) statuses[2] += 1
        else if (std.mem.eql(u8, code, "503")) statuses[3] += 1
        else {
            other_status += 1;
            malformed += 1;
        }
    }
}

fn drop(s: *Slot) void {
    if (s.c) |c| c.close();
    s.c = null;
    s.owed = 0;
}

// ------------------------------------------------------------------ actions

fn actConnect(s: *Slot, port: u16, r: std.Random) void {
    if (s.c != null) drop(s);
    const c = Client.connect(port) catch return;
    c.setNonblocking();
    // A third of them cannot take much at a time, so the server meets a full
    // window while other connections are still being driven.
    if (r.boolean()) c.throttleReceive(2048);
    s.c = c;
}

fn actRequest(s: *Slot, r: std.Random) void {
    const c = s.c orelse return;
    var buf: [hc.work_request_bytes]u8 = undefined;
    const req = hc.workRequest(r.uintLessThan(usize, 900), &buf);
    if (c.sendSome(req) == req.len) s.owed += 1;
}

fn actPipeline(s: *Slot, r: std.Random) void {
    const c = s.c orelse return;
    const depth = 8 + r.uintLessThan(usize, 56);
    var buf: [hc.work_request_bytes]u8 = undefined;
    var sent: usize = 0;
    for (0..depth) |_| {
        const req = hc.workRequest(r.uintLessThan(usize, 900), &buf);
        if (c.sendSome(req) != req.len) break;
        sent += 1;
    }
    s.owed += sent;
}

/// Half a request, then nothing. The parser holds state and the deadline is
/// the only thing that resolves it.
fn actPartial(s: *Slot) void {
    const c = s.c orelse return;
    _ = c.sendSome("GET /work/0007 HTTP/1.1\r\nHost: x\r\n");
}

fn actGarbage(s: *Slot, r: std.Random) void {
    const c = s.c orelse return;
    var junk: [96]u8 = undefined;
    r.bytes(&junk);
    _ = c.sendSome(&junk);
}

fn actOversized(s: *Slot) void {
    const c = s.c orelse return;
    var big: [4096]u8 = undefined;
    @memset(&big, 'A');
    _ = c.sendSome("GET /");
    _ = c.sendSome(&big);
}

fn actRead(s: *Slot) void {
    const c = s.c orelse return;
    var buf: [16 * 1024]u8 = undefined;
    const n = c.recv(&buf, buf.len, 2);
    if (n == 0) return;
    tally(buf[0..n]);
    s.owed -|= hc.countOf(buf[0..n], "HTTP/1.1 ");
}

fn actReset(s: *Slot) void {
    const c = s.c orelse return;
    c.resetOnClose();
    drop(s);
}

/// Ask for a lot and vanish before any of it comes back.
///
/// A uniformly-chosen reset usually finds an idle connection, where the server
/// notices on the read side and never writes at all. This aims one at a
/// connection with work outstanding, which is the state where the write path
/// meets a peer that is already gone: EPIPE handling, the buffer reclaim, and
/// the ending that is `peer_gone` rather than anything the peer asked for.
///
/// It does NOT test the server's SIGPIPE guard, and no in-process test can.
/// See the note at the top of this file.
fn actLoadThenReset(s: *Slot, port: u16, r: std.Random) void {
    actConnect(s, port, r);
    const c = s.c orelse return;
    var buf: [hc.work_request_bytes]u8 = undefined;
    for (0..32) |_| {
        const req = hc.workRequest(r.uintLessThan(usize, 900), &buf);
        if (c.sendSome(req) != req.len) break;
    }
    c.resetOnClose();
    drop(s);
}

fn actHalfClose(s: *Slot) void {
    const c = s.c orelse return;
    c.halfClose();
}

/// Do nothing, for slightly longer than the deadline.
///
/// Without this the loop has no wall time in it at all: four thousand steps
/// run in a few tens of milliseconds, so no connection is ever idle long
/// enough to expire and the deadline path stays unvisited however small the
/// deadline is. Sitting still is a thing clients do, and it is the only way to
/// reach one of the four answers this server can give.
fn actWait() void {
    hc.sleepMs(@intCast(deadline_ms + 15));
}

fn actShed(port: u16, r: std.Random) void {
    const c = Client.connect(port + 1) catch return;
    defer c.close();
    var cmd: [32]u8 = undefined;
    const n = 1 + r.uintLessThan(usize, 8);
    c.send(std.fmt.bufPrint(&cmd, "shed {d}\n", .{n}) catch return) catch return;
    var buf: [64]u8 = undefined;
    _ = c.recvUntil(&buf, "\n", 1, 1000);
}

// --------------------------------------------------------------------- main

pub fn main(init: std.process.Init.Minimal) !void {
    hc.ignoreSigpipe();

    // Iterate rather than walking `init.args.vector`: that field is UTF-16 code
    // units on Windows, so `std.mem.span` on it is a compile error there.
    var it = try init.args.iterateAllocator(std.heap.page_allocator);
    defer it.deinit();
    var argv: [8][]const u8 = undefined;
    var argc: usize = 0;
    while (it.next()) |a| {
        if (argc == 8) break;
        argv[argc] = a;
        argc += 1;
    }
    // usage: chaos <seed> [io_bufs] [deadline_ms] [steps]
    //
    // The shape of the run is worth varying as well as the seed. A pool of one
    // or two makes every acquire a contest and drives the cleanup reserve,
    // which is a different regime from a pool of thirty-two where exhaustion
    // barely happens; a deadline of 20ms cuts across requests mid-flight where
    // 200ms mostly does not.
    const seed: u64 = if (argc > 1) try std.fmt.parseInt(u64, argv[1], 10) else 0x9e3779b9;
    if (argc > 2) io_bufs = try std.fmt.parseInt(i64, argv[2], 10);
    if (argc > 3) deadline_ms = try std.fmt.parseInt(i64, argv[3], 10);
    if (argc > 4) steps = try std.fmt.parseInt(usize, argv[4], 10);

    server.knobs().io_bufs = io_bufs;
    server.knobs().idle_deadline_ms = deadline_ms;
    const port = try startServer();
    std.debug.print("chaos_test: server on 127.0.0.1:{d}, seed {d}, io_bufs {d}, deadline {d}ms\n", .{
        port, seed, io_bufs, deadline_ms,
    });
    std.debug.print("  (replay with: zig build chaos -- {d} {d} {d} {d})\n", .{ seed, io_bufs, deadline_ms, steps });

    var prng = std.Random.DefaultPrng.init(seed);
    const r = prng.random();

    var audits: usize = 0;
    var step: usize = 0;
    while (step < steps) : (step += 1) {
        const s = &slots[r.uintLessThan(usize, n_slots)];
        switch (r.uintLessThan(u32, 100)) {
            0...19 => actConnect(s, port, r),
            20...44 => actRequest(s, r),
            45...54 => actPipeline(s, r),
            55...59 => actPartial(s),
            60...62 => actGarbage(s, r),
            63...64 => actOversized(s),
            65...84 => actRead(s),
            85...89 => actReset(s),
            90...92 => actHalfClose(s),
            93 => drop(s),
            94...96 => actLoadThenReset(s, port, r),
            97...98 => actWait(),
            else => actShed(port, r),
        }

        // Ask the server whether it still agrees with itself, often, while the
        // mess is at its worst rather than only once it has settled.
        if (step % 250 == 249) {
            audits += 1;
            if (hc.askInvariants(port)) |why| {
                checks_failed = true;
                std.debug.print("  FAIL  invariants held throughout the chaos  step {d}: {s}\n", .{ step, why });
                break;
            }
        }
    }

    // Let go of everything and let the server finish what it was doing.
    for (&slots) |*s| drop(s);
    hc.sleepMs(200);

    check(!checks_failed, "invariants held throughout the chaos", .{ .audits = audits });
    check(audits > 0, "audited the server while it was under load", .{ .audits = audits });
    check(malformed == 0, "every response was one this server can produce", .{
        .malformed = malformed,
        .unknown_status = other_status,
        .of = responses,
    });
    check(responses > 0, "the chaos actually got answers, so it was doing something", .{ .responses = responses });
    check(statuses[0] > 0, "some requests were served", .{ .ok = statuses[0] });
    check(statuses[3] > 0, "and some were refused, which is the point of the small pool", .{ .refused = statuses[3] });

    std.debug.print("  {d} responses: {d} ok, {d} bad request, {d} timeout, {d} refused, over {d} audits\n", .{
        responses, statuses[0], statuses[1], statuses[2], statuses[3], audits,
    });

    hc.checkInvariants(port, "after the dust settled");
    tStillHealthy(port);
    hc.report();
}

fn tStillHealthy(port: u16) void {
    const c = Client.connect(port) catch {
        check(false, "server still accepts connections afterwards", .{});
        return;
    };
    defer c.close();
    var rq: [128]u8 = undefined;
    c.send(hc.request("/work/42", &rq)) catch {};
    var buf: [512]u8 = undefined;
    const n = c.recvUntil(&buf, "units", 1, 5000);
    check(std.mem.indexOf(u8, buf[0..n], "+42") != null, "and answers correctly", .{ .bytes = n });
}
