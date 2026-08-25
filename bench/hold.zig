//! Open N connections, send one request each, then hold them open and idle.
//! That is the keep-alive population a real server carries, and the case a
//! per-connection buffer array pays for and a pool does not.
const std = @import("std");
const linux = std.os.linux;
const sockaddr_in = extern struct { family: u16 = linux.AF.INET, port: u16, addr: u32, zero: [8]u8 = @splat(0) };
fn sysErr(rc: usize) bool { return @as(isize, @bitCast(rc)) < 0; }
pub fn main(init: std.process.Init.Minimal) !void {
    var argv: [5][]const u8 = undefined; var argc: usize = 0;
    for (init.args.vector) |a| { if (argc == 5) break; argv[argc] = std.mem.span(a); argc += 1; }
    const port = try std.fmt.parseInt(u16, argv[1], 10);
    const n = try std.fmt.parseInt(usize, argv[2], 10);
    const hold_s = try std.fmt.parseInt(i64, argv[3], 10);
    const gpa = std.heap.page_allocator;
    const fds = try gpa.alloc(i32, n);
    const addr = sockaddr_in{ .port = std.mem.nativeToBig(u16, port), .addr = 0x0100007f };
    var ok: usize = 0;
    for (fds) |*fd| {
        const rc = linux.socket(linux.AF.INET, linux.SOCK.STREAM, 0);
        if (sysErr(rc)) break;
        fd.* = @intCast(rc);
        if (sysErr(linux.connect(fd.*, @ptrCast(&addr), @sizeOf(sockaddr_in)))) break;
        const req = "GET /work/0 HTTP/1.1\r\nHost: x\r\n\r\n";
        _ = linux.write(fd.*, req, req.len);
        var buf: [256]u8 = undefined;
        _ = linux.read(fd.*, &buf, buf.len);
        ok += 1;
    }
    std.debug.print("held {d} idle connections\n", .{ok});
    var ts = linux.timespec{ .sec = hold_s, .nsec = 0 };
    _ = linux.nanosleep(&ts, null);
    for (fds[0..ok]) |fd| _ = linux.close(fd);
}
