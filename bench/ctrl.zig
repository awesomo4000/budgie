//! Simulates a TUI hammering the control surface: send one byte, block until
//! it comes back, record the round trip. That round trip IS the keypress-to-
//! redraw latency of the control surface, measured through the real scheduler
//! while it is under full serving load.
//!
//! usage: ctrl <port> <seconds> <interval_ms>

const std = @import("std");
const budgie = @import("budgie");
const sys = budgie.sys;

// The shim's, because the BSDs put a length byte in front of the family.
const sockaddr_in = sys.SockAddrIn;
fn sysErr(rc: usize) bool {
    return @as(isize, @bitCast(rc)) < 0;
}
fn nowNs() i64 { return budgie.clock.monotonicNs(); }

pub fn main(init: std.process.Init.Minimal) !void {
    var argv: [6][]const u8 = undefined;
    var argc: usize = 0;
    for (init.args.vector) |a| {
        if (argc == 6) break;
        argv[argc] = std.mem.span(a);
        argc += 1;
    }
    const port = try std.fmt.parseInt(u16, argv[1], 10);
    const secs = try std.fmt.parseInt(i64, argv[2], 10);
    const iv_ms = if (argc > 3) try std.fmt.parseInt(i64, argv[3], 10) else 10;

    const rc = sys.tcpSocket();
    if (sysErr(rc)) return error.Socket;
    const fd: i32 = @intCast(rc);
    const addr = sockaddr_in{ .port = std.mem.nativeToBig(u16, port), .addr = 0x0100007f };
    if (sysErr(sys.connect(fd, &addr))) return error.Connect;
    // TCP_NODELAY: Nagle would add its own latency and hide the scheduler's.
    sys.setNoDelay(fd);

    var lat = std.ArrayList(i64).empty;
    const gpa = std.heap.page_allocator;
    const stop = nowNs() + secs * 1_000_000_000;
    var key: u8 = 'k';
    var buf: [8]u8 = undefined;
    while (nowNs() < stop) {
        const t0 = nowNs();
        if (sysErr(sys.write(fd, @ptrCast(&key), 1))) break;
        const r = sys.read(fd, &buf, buf.len);
        if (sysErr(r) or r == 0) break;
        try lat.append(gpa, nowNs() - t0);
        sys.sleepMs(@intCast(iv_ms));
    }
    _ = sys.close(fd);

    const v = lat.items;
    if (v.len == 0) {
        std.debug.print("CTRL,0,0,0,0,0,0\n", .{});
        return;
    }
    std.mem.sort(i64, v, {}, std.sort.asc(i64));
    const q = struct {
        fn f(x: []i64, r: f64) i64 {
            return x[@intFromFloat(r * @as(f64, @floatFromInt(x.len - 1)))];
        }
    }.f;
    // n, p50, p90, p99, p999, max  (nanoseconds)
    std.debug.print("CTRL,{d},{d},{d},{d},{d},{d}\n", .{
        v.len, q(v, 0.50), q(v, 0.90), q(v, 0.99), q(v, 0.999), v[v.len - 1],
    });
}
