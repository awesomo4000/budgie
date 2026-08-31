//! What `app/server.zig` says when it runs out of buffers.
//!
//! Separate from `server_test.zig` because the pool size is fixed at startup,
//! and this needs a pool small enough to exhaust on purpose.
//!
//! The claim being tested is the one `iobuf.zig` opens with: sizing the pool
//! is the memory budget, and exhaustion is an admission decision with a real
//! answer rather than an allocation failure. An admission decision the client
//! never hears about is not one, so the bar here is that every connection gets
//! an answer, and that the answer says what actually happened.
//!
//! Both halves of that were broken when this was written. Under exhaustion the
//! unwind asked the empty pool for a buffer, got nothing, and closed in
//! silence: 48 connections against a pool of 2 produced 43 `no_buffer` endings
//! and zero delivered responses. And `.no_buffer` was missing from the switch
//! that picks the status line, so the few answers that did land said "408
//! Request Timeout" to clients whose requests had not timed out.

const std = @import("std");
const server = @import("server");
const hc = @import("httpclient");

const Client = hc.Client;
const check = hc.check;

/// Small enough that a handful of simultaneous connections cannot all hold
/// one, which is the state worth testing.
const pool_size = 2;
const n_conns = 24;


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
fn buffersBalanced() bool {
    const st = server.stats();
    return st.buf_acquires == st.buf_releases;
}

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
    // Set before the thread starts, so the pool is sized when `start` runs.
    server.knobs().io_bufs = pool_size;
    const port = try startServer();

    std.debug.print("starve_test: server on 127.0.0.1:{d}, io_bufs={d}\n", .{ port, pool_size });

    var clients: [n_conns]Client = undefined;
    var opened: usize = 0;
    defer for (clients[0..opened]) |c| c.close();
    while (opened < n_conns) : (opened += 1) {
        clients[opened] = Client.connect(port) catch break;
    }
    check(opened == n_conns, "opened every connection", .{ .opened = opened, .want = n_conns });

    // Send on all of them before reading any, so the server finds them all
    // readable in the same pass and the pool is contended rather than reused.
    var rq: [128]u8 = undefined;
    const req = hc.request("/work/900", &rq);
    var sent: usize = 0;
    for (clients[0..opened]) |c| {
        c.send(req) catch continue;
        sent += 1;
    }
    check(sent == opened, "sent on every connection", .{ .sent = sent });

    var answered: usize = 0;
    var silent: usize = 0;
    var served: usize = 0;
    var refused: usize = 0;
    var mislabelled: usize = 0;

    for (clients[0..opened]) |c| {
        var buf: [1024]u8 = undefined;
        const n = c.recvUntil(&buf, "\r\n\r\n", 1, 5000);
        const got = buf[0..n];
        if (n == 0) {
            silent += 1;
            continue;
        }
        answered += 1;
        if (std.mem.indexOf(u8, got, "200 OK") != null) served += 1;
        if (std.mem.indexOf(u8, got, "503") != null) refused += 1;
        // A pool that ran dry is not a request that timed out. This is the
        // arm that used to be missing.
        if (std.mem.indexOf(u8, got, "408") != null) mislabelled += 1;
    }

    check(silent == 0, "no connection closed without an answer", .{ .silent = silent, .of = opened });
    check(answered == opened, "every connection got an answer", .{ .answered = answered, .of = opened });
    check(refused > 0, "exhaustion actually happened", .{ .refused = refused, .served = served });
    check(mislabelled == 0, "exhaustion answered 503, never 408", .{ .mislabelled = mislabelled });

    _ = hc.waitUntil(buffersBalanced, 3000);
    const st = server.stats();
    check(st.ended_no_buffer > 0, "no_buffer recorded as its own ending", .{ .no_buffer = st.ended_no_buffer });
    check(st.buf_acquires == st.buf_releases, "buffers balanced under exhaustion", .{
        .acquires = st.buf_acquires,
        .releases = st.buf_releases,
    });
    check(st.bufs_stranded == 0, "and none of them had to be reclaimed behind a connection", .{
        .stranded = st.bufs_stranded,
    });
    check(st.parked_nowhere == 0, "and none parked with nothing able to wake them", .{
        .stuck = st.parked_nowhere,
    });

    std.debug.print("  ({d} served, {d} refused, {d} exhaustions)\n", .{ served, refused, st.buf_exhausted });
    hc.checkInvariants(port, "after exhaustion");
    hc.report();
}
