//! A load generator that is not the bottleneck.
//!
//! The previous one had two defects that invalidated every pipelined
//! measurement taken with it:
//!
//!   1. It counted responses by scanning for "HTTP/1.1" and then discarded the
//!      read buffer, so any response split across two reads was miscounted and
//!      the outstanding-request accounting desynchronised.
//!   2. It issued one write() per request with TCP_NODELAY set, so a depth-32
//!      pipeline went out as 32 segments and arrived as 32 separate events.
//!      The server never saw a batch because the client never sent one.
//!
//! This one frames responses properly (Content-Length, byte-exact, partials
//! carried across reads), submits pipelined requests as a single write, and
//! uses io_uring with multishot recv so both ends of the benchmark have the
//! same blocking behaviour. It also reports its OWN context switches, because
//! on a single core the client's wakeups are half the story.
//!
//! usage: gen <port> <conns> <secs> <depth> <units> [nodelay]

const std = @import("std");
const linux = std.os.linux;
const IoUring = linux.IoUring;

const sockaddr_in = extern struct {
    family: u16 = linux.AF.INET,
    port: u16,
    addr: u32,
    zero: [8]u8 = @splat(0),
};
fn sysErr(rc: usize) bool {
    return @as(isize, @bitCast(rc)) < 0;
}
fn nowNs() i64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, @intCast(ts.sec)) * 1_000_000_000 + @as(i64, @intCast(ts.nsec));
}

// ---------------------------------------------------------------- framing

/// Byte-exact HTTP response framing. Carries partial state across reads, which
/// is the whole reason the old counter was wrong.
const RespParser = struct {
    state: enum { head, body } = .head,
    hdr: [512]u8 = undefined,
    n: usize = 0,
    body_left: usize = 0,
    malformed: u64 = 0,

    fn contentLength(h: []const u8) usize {
        const key = "Content-Length:";
        const p = std.mem.indexOf(u8, h, key) orelse return 0;
        var i = p + key.len;
        while (i < h.len and h[i] == ' ') i += 1;
        var v: usize = 0;
        while (i < h.len and h[i] >= '0' and h[i] <= '9') : (i += 1) v = v * 10 + (h[i] - '0');
        return v;
    }

    /// Returns how many complete responses this slice finished.
    fn feed(p: *RespParser, data: []const u8) u32 {
        var off: usize = 0;
        var done: u32 = 0;
        while (off < data.len) {
            switch (p.state) {
                .head => {
                    const already = p.n;
                    const room = p.hdr.len - p.n;
                    if (room == 0) { // pathological header; resync
                        p.malformed += 1;
                        p.n = 0;
                        return done;
                    }
                    const take = @min(room, data.len - off);
                    @memcpy(p.hdr[p.n..][0..take], data[off..][0..take]);
                    const search_from = if (already >= 3) already - 3 else 0;
                    p.n += take;
                    if (std.mem.indexOfPos(u8, p.hdr[0..p.n], search_from, "\r\n\r\n")) |end| {
                        const hdr_len = end + 4;
                        p.body_left = contentLength(p.hdr[0..end]);
                        off += hdr_len - already; // bytes of THIS slice used
                        p.n = 0;
                        p.state = .body;
                        if (p.body_left == 0) {
                            done += 1;
                            p.state = .head;
                        }
                    } else {
                        off += take;
                        return done; // need more bytes
                    }
                },
                .body => {
                    const take = @min(p.body_left, data.len - off);
                    off += take;
                    p.body_left -= take;
                    if (p.body_left == 0) {
                        done += 1;
                        p.state = .head;
                    }
                },
            }
        }
        return done;
    }
};

// ------------------------------------------------------------------ client

const max_conns = 8192;
const depth_ring = 256;

const Conn = struct {
    fd: i32 = -1,
    parser: RespParser = .{},
    outstanding: u32 = 0,
    sent_at: [depth_ring]i64 = @splat(0),
    head: u32 = 0,
    tail: u32 = 0,
    recv_armed: bool = false,
};

var conns: [max_conns]Conn = undefined;

const tag_recv: u64 = 0;
const tag_send: u64 = 1 << 40;
const tag_mask: u64 = 0xffff_ffff;

pub fn main(init: std.process.Init.Minimal) !void {
    var argv: [8][]const u8 = undefined;
    var argc: usize = 0;
    for (init.args.vector) |a| {
        if (argc == 8) break;
        argv[argc] = std.mem.span(a);
        argc += 1;
    }
    const gpa = std.heap.page_allocator;
    const port = try std.fmt.parseInt(u16, argv[1], 10);
    const nconn = try std.fmt.parseInt(usize, argv[2], 10);
    const secs = try std.fmt.parseInt(i64, argv[3], 10);
    const depth: u32 = if (argc > 4) try std.fmt.parseInt(u32, argv[4], 10) else 1;
    const units: u32 = if (argc > 5) try std.fmt.parseInt(u32, argv[5], 10) else 0;
    const nodelay = argc > 6;

    var rb: [64]u8 = undefined;
    const req = try std.fmt.bufPrint(&rb, "GET /work/{d} HTTP/1.1\r\nHost: x\r\n\r\n", .{units});
    // The entire pipeline burst as ONE buffer, so it goes out in one write.
    const burst = try gpa.alloc(u8, req.len * depth);
    for (0..depth) |i| @memcpy(burst[i * req.len ..][0..req.len], req);

    var ring = IoUring.init(4096, linux.IORING_SETUP_SINGLE_ISSUER | linux.IORING_SETUP_DEFER_TASKRUN) catch
        try IoUring.init(4096, 0);
    var group = try IoUring.BufferGroup.init(&ring, gpa, 1, 4096, 4096);

    const addr = sockaddr_in{ .port = std.mem.nativeToBig(u16, port), .addr = 0x0100007f };
    for (0..nconn) |i| {
        const rc = linux.socket(linux.AF.INET, linux.SOCK.STREAM, 0);
        if (sysErr(rc)) return error.Socket;
        conns[i] = .{ .fd = @intCast(rc) };
        if (sysErr(linux.connect(conns[i].fd, @ptrCast(&addr), @sizeOf(sockaddr_in)))) return error.Connect;
        _ = linux.fcntl(conns[i].fd, 4, 0o4000); // O_NONBLOCK
        if (nodelay) {
            const one: c_int = 1;
            _ = linux.setsockopt(conns[i].fd, 6, 1, @ptrCast(&one), @sizeOf(c_int));
        }
        _ = group.recv_multishot(tag_recv | @as(u64, i), conns[i].fd, 0) catch {};
        conns[i].recv_armed = true;
    }

    var lat = std.ArrayList(i64).empty;
    var total: u64 = 0;
    var enters: u64 = 0;
    var cqes_total: u64 = 0;
    var malformed: u64 = 0;
    const stop = nowNs() + secs * 1_000_000_000;

    while (nowNs() < stop) {
        // Top up: one write per connection carrying the whole burst.
        for (0..nconn) |i| {
            const c = &conns[i];
            if (c.outstanding > 0) continue;
            if (!c.recv_armed) {
                _ = group.recv_multishot(tag_recv | @as(u64, i), c.fd, 0) catch {};
                c.recv_armed = true;
            }
            const w = linux.write(c.fd, burst.ptr, burst.len);
            if (sysErr(w) or w != burst.len) continue;
            const t0 = nowNs();
            for (0..depth) |_| {
                c.sent_at[c.tail % depth_ring] = t0;
                c.tail += 1;
                c.outstanding += 1;
            }
        }

        var ts: linux.kernel_timespec = .{ .sec = 0, .nsec = 2_000_000 };
        _ = ring.timeout(1 << 41, &ts, 0, 0) catch {};
        _ = ring.submit_and_wait(1) catch break;
        enters += 1;

        var cqes: [1024]linux.io_uring_cqe = undefined;
        const n = ring.copy_cqes(&cqes, 0) catch 0;
        cqes_total += n;
        for (cqes[0..n]) |cqe| {
            if (cqe.user_data == 1 << 41) continue;
            if (cqe.user_data & tag_send != 0) continue;
            const i: usize = @intCast(cqe.user_data & tag_mask);
            const c = &conns[i];
            if (cqe.flags & linux.IORING_CQE_F_MORE == 0) c.recv_armed = false;
            if (cqe.res <= 0) continue;
            const data = group.get(cqe) catch continue;
            const done = c.parser.feed(data);
            group.put(cqe) catch {};
            if (done == 0) continue;
            const now = nowNs();
            var k: u32 = 0;
            while (k < done and c.outstanding > 0) : (k += 1) {
                lat.append(gpa, now - c.sent_at[c.head % depth_ring]) catch {};
                c.head += 1;
                c.outstanding -= 1;
                total += 1;
            }
        }
    }
    for (0..nconn) |i| malformed += conns[i].parser.malformed;

    // Our own context switches: on one core the client's wakeups are half the
    // story and reporting only the server's hides that.
    var vol: u64 = 0;
    var nonvol: u64 = 0;
    {
        var buf: [4096]u8 = undefined;
        const fd = linux.open("/proc/self/status", .{ .ACCMODE = .RDONLY }, 0);
        if (!sysErr(fd)) {
            const r = linux.read(@intCast(fd), &buf, buf.len);
            _ = linux.close(@intCast(fd));
            if (!sysErr(r)) {
                var it = std.mem.tokenizeAny(u8, buf[0..r], "\n");
                while (it.next()) |line| {
                    if (std.mem.startsWith(u8, line, "voluntary_ctxt_switches:"))
                        vol = std.fmt.parseInt(u64, std.mem.trim(u8, line[24..], " \t"), 10) catch 0;
                    if (std.mem.startsWith(u8, line, "nonvoluntary_ctxt_switches:"))
                        nonvol = std.fmt.parseInt(u64, std.mem.trim(u8, line[27..], " \t"), 10) catch 0;
                }
            }
        }
    }

    const v = lat.items;
    std.mem.sort(i64, v, {}, std.sort.asc(i64));
    const q = struct {
        fn f(x: []i64, r: f64) i64 {
            if (x.len == 0) return 0;
            return x[@intFromFloat(r * @as(f64, @floatFromInt(x.len - 1)))];
        }
    }.f;
    // conns,depth,total,rate,p50ns,p99ns,maxns,cli_vol_ctxsw,cli_nonvol,enters,cqes,malformed
    std.debug.print("GEN,{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d}\n", .{
        nconn, depth, total, @divTrunc(@as(i64, @intCast(total)), secs),
        q(v, 0.50), q(v, 0.99), q(v, 1.0), vol, nonvol, enters, cqes_total, malformed,
    });
}
