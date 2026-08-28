//! Cancellation over a real socket, which until now had never happened.
//!
//! Everything about cancellation in this tree was exercised against the
//! scheduler directly. `tests/cancel_test.zig` drives the API, `wrong_cancel`
//! catalogues the mistakes, the two examples race four tasks in a loop with no
//! I/O in it. None of that touches a connection. `.cancelled` was a reachable
//! `Ending` with a 503 already written for it and no caller anywhere, which is
//! the exact shape of the `.no_buffer` bug this suite found earlier: a status
//! nobody had ever seen delivered.
//!
//! So this drives it end to end. The control surface takes `shed <n>`, the
//! server cancels that many connections using tokens it was handed at
//! admission, and the clients on the other end have to be told.
//!
//! What makes shedding worth testing rather than contrived: it is the one
//! ending a deadline cannot produce. Every connection here is healthy,
//! responsive and inside its budget. Nothing has gone wrong with any of them.
//! Somebody outside decided there should be fewer, which is a decision no
//! amount of watching the connection could ever reach.

const std = @import("std");
const server = @import("server");
const hc = @import("httpclient");

const Client = hc.Client;
const check = hc.check;

/// Long enough that nothing here can be mistaken for a deadline expiry, which
/// is the other thing that answers a connection and closes it.
const deadline_ms = 30_000;
const wait_ms = 5000;

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

/// Ask the control surface to shed `n`, and return what it says it shed.
///
/// Takes an already-open control connection rather than dialling one. A fresh
/// connect costs an accept on a server that is busy grinding through a
/// pipeline, and that delay was long enough on the completion backend for the
/// whole burst to finish before the order arrived, which made the mid-flight
/// scene below assert nothing. It is also how an operator would actually hold
/// a control channel.
fn shed(c: Client, n: usize) !usize {
    var cmd: [32]u8 = undefined;
    try c.send(std.fmt.bufPrint(&cmd, "shed {d}\n", .{n}) catch unreachable);
    var buf: [64]u8 = undefined;
    const got = c.recvUntil(&buf, "\n", 1, wait_ms);
    const line = buf[0..got];
    if (!std.mem.startsWith(u8, line, "shed ")) return error.BadReply;
    var end: usize = 5;
    while (end < line.len and line[end] >= '0' and line[end] <= '9') end += 1;
    return std.fmt.parseInt(usize, line[5..end], 10) catch error.BadReply;
}

/// Open `n` keep-alive connections and get one answer on each, so every one of
/// them is established, admitted, idle and demonstrably working.
fn openIdle(port: u16, clients: []Client) !usize {
    var opened: usize = 0;
    while (opened < clients.len) : (opened += 1) {
        clients[opened] = Client.connect(port) catch break;
        var rq: [128]u8 = undefined;
        clients[opened].send(hc.request("/work/1", &rq)) catch break;
        var buf: [256]u8 = undefined;
        const got = clients[opened].recvUntil(&buf, "units", 1, wait_ms);
        if (std.mem.indexOf(u8, buf[0..got], "spent") == null) break;
    }
    return opened;
}

/// The whole point: a healthy idle connection, cancelled, is told so.
fn tShedTellsTheClient(port: u16, ctrl: Client) !void {
    var clients: [8]Client = undefined;
    const opened = try openIdle(port, &clients);
    defer for (clients[0..opened]) |c| c.close();
    check(opened == clients.len, "opened and served every connection first", .{ .opened = opened });

    const before = server.stats();
    const n = try shed(ctrl, opened);
    check(n == opened, "the control surface shed every one of them", .{ .shed = n, .of = opened });

    var answered: usize = 0;
    var silent: usize = 0;
    var mislabelled: usize = 0;
    for (clients[0..opened]) |c| {
        var buf: [512]u8 = undefined;
        const got = c.recvUntil(&buf, "\r\n\r\n", 1, wait_ms);
        if (got == 0) {
            silent += 1;
            continue;
        }
        answered += 1;
        if (hc.statusIs(buf[0..got], "503")) continue;
        mislabelled += 1;
        std.debug.print("        got: {s}\n", .{hc.statusLine(buf[0..got])});
    }

    // Silence is the failure this whole design is arguing against. A cancelled
    // connection that just stops is indistinguishable from one that hung.
    check(silent == 0, "no cancelled connection was closed without a word", .{ .silent = silent });
    check(answered == opened, "every cancelled connection got an answer", .{ .answered = answered });
    check(mislabelled == 0, "and the answer was 503, not 408 and not silence", .{ .wrong = mislabelled });

    const after = server.stats();
    check(after.ended_cancelled - before.ended_cancelled == opened, "counted as cancelled, its own ending", .{
        .cancelled = after.ended_cancelled - before.ended_cancelled,
        .want = opened,
    });
    // The deadline is 30 seconds out. If any of these came back as a deadline
    // the test would be measuring the wrong mechanism entirely.
    check(after.ended_deadline == before.ended_deadline, "and not as a deadline, which is 30s away", .{
        .deadline = after.ended_deadline - before.ended_deadline,
    });
}

/// A connection with work in flight, not an idle one. The cancel arrives while
/// the task is queued and mid-request, which is the case where reading the wake
/// reason instead of the state would silently miss it.
///
/// One request is not enough to catch: `/work/900` is four dispatches and the
/// control round trip is slower than that, so the connection is idle again
/// before the shed lands. A deep pipeline keeps it genuinely busy, and the
/// server's own `served` counter is what proves it rather than the client's
/// transcript, for a reason worth writing down. See below.
fn tShedMidRequest(port: u16, ctrl: Client) !void {
    const depth = 400;
    const c = try Client.connect(port);
    defer c.close();

    // Warm up first, and wait for the answer. `connect` returning only means
    // the kernel completed the handshake; the server has not necessarily run
    // `accept` yet, so the task does not exist and there is nothing to shed.
    // On a fast machine the whole scene raced past this: shed found no live
    // connection, returned zero, and the server then served all 400.
    var rq: [128]u8 = undefined;
    try c.send(hc.request("/work/1", &rq));
    var warm: [256]u8 = undefined;
    const warmed = c.recvUntil(&warm, "units", 1, wait_ms);
    check(std.mem.indexOf(u8, warm[0..warmed], "spent") != null, "connection is accepted and serving before we shed it", .{});

    const burst = try std.heap.page_allocator.alloc(u8, depth * hc.work_request_bytes);
    defer std.heap.page_allocator.free(burst);
    for (0..depth) |i| _ = hc.workRequest(900, burst[i * hc.work_request_bytes ..][0..hc.work_request_bytes]);

    const before = server.stats();
    try c.send(burst);

    // And wait until it has actually started on the burst, so "mid-flight"
    // is a fact rather than a hope about timing.
    var spun: usize = 0;
    while (server.stats().served == before.served and spun < 2000) : (spun += 1) hc.sleepMs(1);
    check(server.stats().served > before.served, "and has started on the pipeline", .{ .spun = spun });

    const n = try shed(ctrl, 1);
    check(n >= 1, "shed a connection that was in the middle of working", .{ .shed = n });

    const reply = try std.heap.page_allocator.alloc(u8, depth * 96 + 4096);
    defer std.heap.page_allocator.free(reply);
    const got = c.recvUntil(reply, "503", 1, wait_ms);
    const seen = reply[0..got];
    const after = server.stats();

    const computed = after.served - before.served;
    const delivered = hc.countOf(seen, hc.ok_head);

    check(n >= 1, "the busy connection was shed", .{ .shed = n });
    check(std.mem.indexOf(u8, seen, "503 Service Unavailable") != null, "and the client is told, not left waiting", .{
        .bytes = got,
    });
    check(after.ended_cancelled > before.ended_cancelled, "counted as cancelled", .{});

    // Whether the cancel lands part-way through the pipeline is not asserted,
    // because it is not a property of cancellation. It is a property of how
    // promptly each backend services a priority-zero control task while a
    // connection is grinding, and the two disagree. Measured here: the
    // readiness server catches it mid-pipeline, and the completion server
    // serves all 400 requests, roughly 72 million multiply-adds, before it
    // reads a single byte from an already-open control connection. That is
    // worth chasing somewhere it can be measured properly rather than being
    // smuggled into a test about something else.
    //
    // When it does land mid-pipeline, the discard below is visible: batching
    // appends answers into one buffer, and a cancel arriving before that write
    // formats the 503 from index zero and takes them with it. Work charged
    // for, done, and thrown away. Defensible for a load shed, and the first
    // thing to revisit if that stops being true.
    std.debug.print("    computed {d} of {d}, delivered {d}{s}\n", .{
        computed, depth, delivered,
        if (delivered < computed) " (answers discarded with the unwind)" else " (finished before the order arrived)",
    });
}

/// The control connection issuing the order must not shed itself, or the reply
/// has nowhere to go.
fn tCtrlDoesNotShedItself(port: u16, ctrl: Client) !void {
    const c = try Client.connect(port);
    defer c.close();
    var rq: [128]u8 = undefined;
    try c.send(hc.request("/work/1", &rq));
    var buf: [256]u8 = undefined;
    _ = c.recvUntil(&buf, "units", 1, wait_ms);

    // Ask for far more than exist. Only the one worker connection qualifies.
    const n = try shed(ctrl, 1000);
    check(n == 1, "shedding everything sheds the workers and not the control connection", .{ .shed = n });
}

fn buffersBalanced() bool {
    const st = server.stats();
    return st.buf_acquires == st.buf_releases;
}

fn tAccounting() !void {
    _ = hc.waitUntil(buffersBalanced, 5000);
    const st = server.stats();
    check(st.buf_acquires == st.buf_releases and st.buf_live == 0, "every buffer released across the cancels", .{
        .acquires = st.buf_acquires,
        .releases = st.buf_releases,
        .live = st.buf_live,
    });
    check(st.cancels_stale == 0, "no stale token was ever presented", .{ .stale = st.cancels_stale });
}

fn tStillHealthy(port: u16) !void {
    const c = try Client.connect(port);
    defer c.close();
    var rq: [128]u8 = undefined;
    try c.send(hc.request("/work/33", &rq));
    var buf: [512]u8 = undefined;
    const got = c.recvUntil(&buf, "units", 1, wait_ms);
    check(std.mem.indexOf(u8, buf[0..got], "+33") != null, "server healthy after shedding", got);
}

pub fn main() !void {
    hc.ignoreSigpipe();
    server.knobs().idle_deadline_ms = deadline_ms;
    const port = try startServer();
    const ctrl_port = port + 1;
    std.debug.print("cancel_socket_test: server on 127.0.0.1:{d}, control on {d}\n", .{ port, ctrl_port });

    // One control connection for the whole run, opened before any load.
    const ctrl = try Client.connect(ctrl_port);
    defer ctrl.close();

    try tShedTellsTheClient(port, ctrl);
    try tShedMidRequest(port, ctrl);
    try tCtrlDoesNotShedItself(port, ctrl);
    try tStillHealthy(port);
    try tAccounting();

    hc.report();
}
