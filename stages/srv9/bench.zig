//! Load generator. N persistent keep-alive connections, each doing
//! request -> response -> repeat for D seconds. Records per-request latency.
//!
//! usage: bench <port> <conns> <seconds> <slow_conns> <fast_units> <slow_units>

const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;

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

const C = struct {
    fd: i32,
    slow: bool,
    sending: bool = true,
    sent: usize = 0,
    req_len: usize = 0,
    req: [64]u8 = undefined,
    got: usize = 0,
    buf: [1024]u8 = undefined,
    start_ns: i64 = 0,
    done: u64 = 0,
};

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
    const nslow = if (argc > 4) try std.fmt.parseInt(usize, argv[4], 10) else 0;
    const fast_u = if (argc > 5) try std.fmt.parseInt(u32, argv[5], 10) else 0;
    const slow_u = if (argc > 6) try std.fmt.parseInt(u32, argv[6], 10) else 900;

    const conns = try gpa.alloc(C, nconn);
    const fds = try gpa.alloc(posix.pollfd, nconn);
    var fast_lat = std.ArrayList(i64).empty;
    var slow_lat = std.ArrayList(i64).empty;

    const addr = sockaddr_in{ .port = std.mem.nativeToBig(u16, port), .addr = 0x0100007f };
    for (conns, 0..) |*c, i| {
        const rc = linux.socket(linux.AF.INET, linux.SOCK.STREAM, 0);
        if (sysErr(rc)) return error.SocketFailed;
        const fd: i32 = @intCast(rc);
        if (sysErr(linux.connect(fd, @ptrCast(&addr), @sizeOf(sockaddr_in)))) return error.ConnectFailed;
        _ = linux.fcntl(fd, 4, 0o4000); // F_SETFL O_NONBLOCK
        c.* = .{ .fd = fd, .slow = i < nslow };
        const units = if (c.slow) slow_u else fast_u;
        c.req_len = (try std.fmt.bufPrint(&c.req, "GET /work/{d} HTTP/1.1\r\nHost: x\r\n\r\n", .{units})).len;
        c.start_ns = nowNs();
    }

    const stop = nowNs() + secs * 1_000_000_000;
    var total: u64 = 0;
    while (nowNs() < stop) {
        for (conns, 0..) |*c, i| {
            fds[i] = .{
                .fd = c.fd,
                .events = if (c.sending) posix.POLL.OUT else posix.POLL.IN,
                .revents = 0,
            };
        }
        _ = posix.poll(fds, 100) catch 0;
        for (conns, 0..) |*c, i| {
            if (fds[i].revents == 0) continue;
            if (c.sending) {
                const rc = linux.write(c.fd, c.req[c.sent..].ptr, c.req_len - c.sent);
                if (sysErr(rc)) continue;
                c.sent += rc;
                if (c.sent == c.req_len) {
                    c.sending = false;
                    c.got = 0;
                }
            } else {
                const rc = linux.read(c.fd, c.buf[c.got..].ptr, c.buf.len - c.got);
                if (sysErr(rc)) continue;
                if (rc == 0) continue;
                c.got += rc;
                if (std.mem.indexOf(u8, c.buf[0..c.got], "\r\n\r\n") != null) {
                    const lat = nowNs() - c.start_ns;
                    if (c.slow) try slow_lat.append(gpa, lat) else try fast_lat.append(gpa, lat);
                    total += 1;
                    c.done += 1;
                    c.sending = true;
                    c.sent = 0;
                    c.start_ns = nowNs();
                }
            }
        }
    }

    const elapsed = @as(f64, @floatFromInt(secs));
    const csv = argc > 7;
    if (csv) {
        const fs = gpa.dupe(i64, fast_lat.items) catch fast_lat.items;
        std.mem.sort(i64, fs, {}, std.sort.asc(i64));
        const sl = gpa.dupe(i64, slow_lat.items) catch slow_lat.items;
        std.mem.sort(i64, sl, {}, std.sort.asc(i64));
        const q = struct {
            fn f(v: []i64, r: f64) i64 {
                if (v.len == 0) return 0;
                return v[@intFromFloat(r * @as(f64, @floatFromInt(v.len - 1)))];
            }
        }.f;
        std.debug.print("BEN,{d},{d},{d},{d},{d},{d},{d},{d},{d},{d}\n", .{
            nconn, nslow, total, @divTrunc(@as(i64, @intCast(total)), secs),
            q(fs, 0.50), q(fs, 0.90), q(fs, 0.99), q(fs, 1.0),
            q(sl, 0.50), q(sl, 0.99),
        });
        return;
    }
    std.debug.print("\nconns={d} (slow={d})  duration={d}s\n", .{ nconn, nslow, secs });
    std.debug.print("total requests = {d}   throughput = {d:.0} req/s\n", .{ total, @as(f64, @floatFromInt(total)) / elapsed });
    report("fast", fast_lat.items, gpa);
    if (nslow > 0) report("slow", slow_lat.items, gpa);
}

fn report(name: []const u8, xs: []i64, gpa: std.mem.Allocator) void {
    if (xs.len == 0) return;
    const s = gpa.dupe(i64, xs) catch return;
    std.mem.sort(i64, s, {}, std.sort.asc(i64));
    const p = struct {
        fn f(v: []i64, q: f64) f64 {
            const idx: usize = @intFromFloat(q * @as(f64, @floatFromInt(v.len - 1)));
            return @as(f64, @floatFromInt(v[idx])) / 1000.0;
        }
    }.f;
    std.debug.print(
        "{s:>5}: n={d:<7} p50={d:>9.1}us  p90={d:>9.1}us  p99={d:>9.1}us  max={d:>9.1}us\n",
        .{ name, s.len, p(s, 0.50), p(s, 0.90), p(s, 0.99), p(s, 1.0) },
    );
}
