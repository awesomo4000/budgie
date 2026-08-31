//! Open N connections, send one request each, then hold them open and idle.
//! That is the keep-alive population a real server carries, and the case a
//! per-connection buffer array pays for and a pool does not.
const std = @import("std");
const budgie = @import("budgie");
const sys = budgie.sys;
// The shim's, because the BSDs put a length byte in front of the family.
const sockaddr_in = sys.SockAddrIn;
fn sysErr(rc: usize) bool { return @as(isize, @bitCast(rc)) < 0; }
pub fn main(init: std.process.Init.Minimal) !void {
    var argv: [5][]const u8 = undefined; var argc: usize = 0;
    var it = try init.args.iterateAllocator(std.heap.page_allocator);
    defer it.deinit();
    while (it.next()) |a| { if (argc == 5) break; argv[argc] = a; argc += 1; }
    const port = try std.fmt.parseInt(u16, argv[1], 10);
    const n = try std.fmt.parseInt(usize, argv[2], 10);
    const hold_s = try std.fmt.parseInt(i64, argv[3], 10);
    const gpa = std.heap.page_allocator;
    const fds = try gpa.alloc(i32, n);
    const addr = sockaddr_in{ .port = std.mem.nativeToBig(u16, port), .addr = 0x0100007f };
    var ok: usize = 0;
    for (fds) |*fd| {
        const rc = sys.tcpSocket();
        if (sysErr(rc)) break;
        fd.* = @intCast(rc);
        if (sysErr(sys.connect(fd.*, &addr))) break;
        const req = "GET /work/0 HTTP/1.1\r\nHost: x\r\n\r\n";
        _ = sys.write(fd.*, req, req.len);
        var buf: [256]u8 = undefined;
        _ = sys.read(fd.*, &buf, buf.len);
        ok += 1;
    }
    std.debug.print("held {d} idle connections\n", .{ok});
    sys.sleepMs(@intCast(hold_s * 1000));
    for (fds[0..ok]) |fd| _ = sys.close(fd);
}
