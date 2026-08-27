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

fn serverThread() void {
    server.runUntil(std.math.maxInt(i64));
}

pub fn main() !void {
    server.knobs().io_bufs = pool_size;
    const port = try server.start(0);
    var thread = try std.Thread.spawn(.{}, serverThread, .{});
    thread.detach();

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

    const st = server.stats();
    check(st.endings[5] > 0, "no_buffer recorded as its own ending", .{ .no_buffer = st.endings[5] });
    check(st.buf_acquires == st.buf_releases, "buffers balanced under exhaustion", .{
        .acquires = st.buf_acquires,
        .releases = st.buf_releases,
    });

    std.debug.print("  ({d} served, {d} refused, {d} exhaustions)\n", .{ served, refused, st.buf_exhausted });
    hc.report();
}
