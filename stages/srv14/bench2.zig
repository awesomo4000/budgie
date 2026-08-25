//! Load generator with pipelining depth. The original kept exactly one request
//! in flight per connection, so the server never had a queue to batch -- which
//! is precisely the condition a completion-based reactor needs.
//! usage: bench2 <port> <conns> <secs> <depth> <units>
const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;
const sockaddr_in = extern struct { family: u16 = linux.AF.INET, port: u16, addr: u32, zero: [8]u8 = @splat(0) };
fn sysErr(rc: usize) bool { return @as(isize, @bitCast(rc)) < 0; }
fn nowNs() i64 { var ts: linux.timespec = undefined; _ = linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64,@intCast(ts.sec))*1_000_000_000 + @as(i64,@intCast(ts.nsec)); }

const C = struct {
    fd: i32,
    outstanding: u32 = 0,
    sent_at: [64]i64 = @splat(0),
    head: u32 = 0,
    tail: u32 = 0,
    rbuf: [8192]u8 = undefined,
    rlen: usize = 0,
};

pub fn main(init: std.process.Init.Minimal) !void {
    var argv: [8][]const u8 = undefined; var argc: usize = 0;
    for (init.args.vector) |a| { if (argc == 8) break; argv[argc] = std.mem.span(a); argc += 1; }
    const gpa = std.heap.page_allocator;
    const port = try std.fmt.parseInt(u16, argv[1], 10);
    const nconn = try std.fmt.parseInt(usize, argv[2], 10);
    const secs = try std.fmt.parseInt(i64, argv[3], 10);
    const depth: u32 = if (argc > 4) try std.fmt.parseInt(u32, argv[4], 10) else 1;
    const units: u32 = if (argc > 5) try std.fmt.parseInt(u32, argv[5], 10) else 0;

    var req_buf: [64]u8 = undefined;
    const req = try std.fmt.bufPrint(&req_buf, "GET /work/{d} HTTP/1.1\r\nHost: x\r\n\r\n", .{units});
    // Pipelined requests must go out in ONE write. With TCP_NODELAY and one
    // write per request, `depth` requests become `depth` segments and arrive
    // as `depth` separate completions -- the client serialises them on the
    // wire and the server never sees a batch to work on.
    const burst = try gpa.alloc(u8, req.len * depth);
    for (0..depth) |i| @memcpy(burst[i * req.len ..][0..req.len], req);

    const conns = try gpa.alloc(C, nconn);
    const fds = try gpa.alloc(posix.pollfd, nconn);
    const addr = sockaddr_in{ .port = std.mem.nativeToBig(u16, port), .addr = 0x0100007f };
    for (conns) |*c| {
        const rc = linux.socket(linux.AF.INET, linux.SOCK.STREAM, 0);
        if (sysErr(rc)) return error.Socket;
        c.* = .{ .fd = @intCast(rc) };
        if (sysErr(linux.connect(c.fd, @ptrCast(&addr), @sizeOf(sockaddr_in)))) return error.Connect;
        _ = linux.fcntl(c.fd, 4, 0o4000);
        const one: c_int = 1;
        _ = linux.setsockopt(c.fd, 6, 1, @ptrCast(&one), @sizeOf(c_int));
    }

    var lat = std.ArrayList(i64).empty;
    var total: u64 = 0;
    const stop = nowNs() + secs * 1_000_000_000;
    while (nowNs() < stop) {
        // top up each connection to `depth` outstanding requests
        for (conns) |*c| {
            if (c.outstanding > 0) continue;
            const w = linux.write(c.fd, burst.ptr, burst.len);
            if (sysErr(w) or w < burst.len) continue;
            const now0 = nowNs();
            for (0..depth) |_| {
                c.sent_at[c.tail % 64] = now0;
                c.tail += 1;
                c.outstanding += 1;
            }
        }
        for (conns, 0..) |*c, i| fds[i] = .{ .fd = c.fd, .events = posix.POLL.IN, .revents = 0 };
        _ = posix.poll(fds, 50) catch 0;
        for (conns, 0..) |*c, i| {
            if (fds[i].revents == 0) continue;
            const r = linux.read(c.fd, c.rbuf[c.rlen..].ptr, c.rbuf.len - c.rlen);
            if (sysErr(r) or r == 0) continue;
            c.rlen += r;
            // count complete responses by their status lines
            var done: usize = 0;
            var idx: usize = 0;
            while (std.mem.indexOfPos(u8, c.rbuf[0..c.rlen], idx, "HTTP/1.1")) |p| {
                idx = p + 8;
                done += 1;
            }
            if (done == 0) continue;
            const now = nowNs();
            var k: usize = 0;
            while (k < done and c.outstanding > 0) : (k += 1) {
                try lat.append(gpa, now - c.sent_at[c.head % 64]);
                c.head += 1;
                c.outstanding -= 1;
                total += 1;
            }
            c.rlen = 0;
        }
    }
    const v = lat.items;
    std.mem.sort(i64, v, {}, std.sort.asc(i64));
    const q = struct { fn f(x: []i64, r: f64) i64 { if (x.len == 0) return 0;
        return x[@intFromFloat(r * @as(f64, @floatFromInt(x.len - 1)))]; } }.f;
    std.debug.print("BEN2,{d},{d},{d},{d},{d},{d}\n", .{ nconn, depth, total, @divTrunc(@as(i64,@intCast(total)), secs), q(v,0.50), q(v,0.99) });
}
