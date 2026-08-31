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

/// Ask the control surface to shed `n`, and return what it says it shed.
///
/// Takes an already-open control connection rather than dialling one. A fresh
/// connect costs an accept on a server that is busy grinding through a
/// pipeline, and that delay was long enough on the completion backend for the
/// whole burst to finish before the order arrived, which made the mid-flight
/// scene below assert nothing. It is also how an operator would actually hold
/// a control channel.
const Shed = struct { cancelled: usize, busy: usize };

fn shed(c: Client, n: usize) !Shed {
    var cmd: [32]u8 = undefined;
    try c.send(std.fmt.bufPrint(&cmd, "shed {d}\n", .{n}) catch @panic("shed command buffer too small"));
    var buf: [64]u8 = undefined;
    const got = c.recvUntil(&buf, "\n", 1, wait_ms);
    const line = buf[0..got];
    if (!std.mem.startsWith(u8, line, "shed ")) return error.BadReply;
    var it = std.mem.tokenizeAny(u8, line, " \r\n");
    _ = it.next(); // "shed"
    const cancelled = std.fmt.parseInt(usize, it.next() orelse return error.BadReply, 10) catch return error.BadReply;
    _ = it.next(); // "busy"
    const busy = std.fmt.parseInt(usize, it.next() orelse return error.BadReply, 10) catch return error.BadReply;
    return .{ .cancelled = cancelled, .busy = busy };
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
    const r = try shed(ctrl, opened);
    check(r.cancelled == opened, "the control surface shed every one of them", .{ .shed = r.cancelled, .of = opened });
    check(r.busy == 0, "and reports them as idle, which is what they were", .{ .busy = r.busy });

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

/// Connections with work actually in flight.
///
/// The first version of this scene sent one deep pipeline and hoped the shed
/// would arrive before it finished. On the readiness backend it did; on the
/// completion backend the connection served all four hundred requests first,
/// so the scene cancelled an idle connection while claiming to have cancelled
/// a busy one. A check whose name does not match what it verified is worse
/// than no check, and calling that a latency finding was dressing it up.
///
/// So the server decides. `shed` reports how many of the connections it
/// cancelled were holding a buffer at the moment it cancelled them, judged in
/// the same pass as the cancel rather than sampled from outside, and that
/// number is what this asserts. Enough load is offered that the answer cannot
/// be zero for want of anything to do: eight connections, four hundred deep,
/// nine hundred units each.
fn tShedBusyConnections(port: u16, ctrl: Client) !void {
    const conns = 8;
    const depth = 400;

    var clients: [conns]Client = undefined;
    var opened: usize = 0;
    defer for (clients[0..opened]) |c| c.close();

    const burst = try std.heap.page_allocator.alloc(u8, depth * hc.work_request_bytes);
    defer std.heap.page_allocator.free(burst);
    for (0..depth) |i| _ = hc.workRequest(900, burst[i * hc.work_request_bytes ..][0..hc.work_request_bytes]);

    // Warm each one up and read the answer, so the task exists server-side.
    // `connect` returning only means the handshake completed; the server has
    // not necessarily run `accept`, and shedding then finds nothing at all.
    while (opened < conns) : (opened += 1) {
        clients[opened] = Client.connect(port) catch break;
        var rq: [128]u8 = undefined;
        clients[opened].send(hc.request("/work/1", &rq)) catch break;
        var warm: [256]u8 = undefined;
        const w = clients[opened].recvUntil(&warm, "units", 1, wait_ms);
        if (std.mem.indexOf(u8, warm[0..w], "spent") == null) break;
    }
    check(opened == conns, "opened and warmed every connection", .{ .opened = opened });

    const before = server.stats();
    for (clients[0..opened]) |c| c.send(burst) catch {};

    // Wait until the server has started on it, so there is work in progress
    // rather than work merely queued in a socket.
    var spun: usize = 0;
    while (server.stats().served == before.served and spun < 3000) : (spun += 1) hc.sleepMs(1);
    check(server.stats().served > before.served, "the server has started on the pipelines", .{ .spun = spun });

    const r = try shed(ctrl, opened);
    check(r.cancelled == opened, "shed all of them", .{ .shed = r.cancelled, .of = opened });
    check(r.busy > 0, "and at least one had I/O in flight when it was cancelled", .{
        .busy = r.busy,
        .of = r.cancelled,
    });

    var told: usize = 0;
    const reply = try std.heap.page_allocator.alloc(u8, depth * 96 + 8192);
    defer std.heap.page_allocator.free(reply);
    for (clients[0..opened]) |c| {
        const got = c.recvUntil(reply, "503", 1, wait_ms);
        if (std.mem.indexOf(u8, reply[0..got], "503 Service Unavailable") != null) told += 1;
    }
    check(told == opened, "every one of them was told, mid-work, rather than left waiting", .{
        .told = told,
        .of = opened,
    });

    // Poll rather than sample. `recvUntil` returns as soon as the bytes "503"
    // appear, which can be the first few bytes of a partial write, and the
    // ending is not counted until that write completes. Sampling immediately
    // read 7 of 8 on a fast machine.
    var settled: usize = 0;
    while (settled < 200) : (settled += 1) {
        if (server.stats().ended_cancelled - before.ended_cancelled == opened) break;
        hc.sleepMs(10);
    }
    const after = server.stats();
    check(after.ended_cancelled - before.ended_cancelled == opened, "counted as cancelled", .{
        .cancelled = after.ended_cancelled - before.ended_cancelled,
        .of = opened,
    });

    // Batching appends answers into one buffer and writes the run in a single
    // call, so a cancel arriving before that write formats the 503 from index
    // zero and takes the finished answers with it. Work charged for, done, and
    // thrown away. Defensible for a load shed, and the first thing to revisit
    // if that stops being true.
    std.debug.print("    {d} of {d} cancelled connections were mid-flight\n", .{ r.busy, r.cancelled });
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
    const r = try shed(ctrl, 1000);
    check(r.cancelled == 1, "shedding everything sheds the workers and not the control connection", .{ .shed = r.cancelled });
}

fn buffersBalanced() bool {
    const st = server.stats();
    // The whole condition, not half of it. Polling only on acquires ==
    // releases and then re-reading `buf_live` left a window: a teardown still
    // in flight satisfies the first and not the second, and this failed once
    // in six Linux runs with acquires 36, releases 35, live 1.
    return st.buf_acquires == st.buf_releases and st.buf_live == 0;
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
    // The pool takes back anything a connection ended still holding. Zero here
    // means every unwind did its own job and the net caught nothing, which is
    // what it should read when the code is right.
    check(st.bufs_stranded == 0, "no buffer had to be reclaimed behind a connection", .{
        .stranded = st.bufs_stranded,
    });
    check(st.parked_nowhere == 0, "no connection parked with nothing able to wake it", .{
        .stuck = st.parked_nowhere,
    });
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
    try tShedBusyConnections(port, ctrl);
    try tCtrlDoesNotShedItself(port, ctrl);
    try tStillHealthy(port);
    try tAccounting();

    hc.checkInvariants(port, "after shedding");
    hc.report();
}
