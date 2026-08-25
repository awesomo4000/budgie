const std = @import("std");
// The OLD feed, verbatim, to show whether the bug was uring-specific.
const OldParser = struct {
    const max_request_bytes = 512;
    state: enum { accumulating, complete, failed } = .accumulating,
    buf: [max_request_bytes]u8 = undefined,
    len: usize = 0,
    scanned: usize = 0,
    fn feed(p: *OldParser, bytes: []const u8) usize {
        if (p.state != .accumulating) return 0;
        const room = max_request_bytes - p.len;
        const take = @min(room, bytes.len);
        @memcpy(p.buf[p.len..][0..take], bytes[0..take]);
        const before = p.len;
        p.len += take;
        const from = if (p.scanned >= 3) p.scanned - 3 else 0;
        if (std.mem.indexOfPos(u8, p.buf[0..p.len], from, "\r\n\r\n")) |end| {
            p.state = .complete;
            return end + 4 - before;
        }
        p.scanned = p.len;
        return take;
    }
};

pub fn main() !void {
    const req = "GET /work/0 HTTP/1.1\r\nHost: x\r\n\r\n";
    std.debug.print("one request = {d} bytes\n\n", .{req.len});

    // Case A: one request at a time (what the ORIGINAL bench client sent,
    // one write() per request with TCP_NODELAY).
    var a: OldParser = .{};
    std.debug.print("A  single request per feed : used={d} (correct = {d})\n", .{ a.feed(req), req.len });

    // Case B: a pipelined burst in ONE feed.
    var burst: [512]u8 = undefined;
    for (0..8) |i| @memcpy(burst[i * req.len ..][0..req.len], req);
    var b: OldParser = .{};
    const u1_ = b.feed(burst[0 .. req.len * 8]);
    std.debug.print("B  8-deep burst, 1st feed  : used={d} (correct = {d})  parser_len={d}\n", .{ u1_, req.len, b.len });

    // Case C: the caller shifts by `used` and feeds the remainder to a FRESH
    // parser, exactly as the servers do.
    var c: OldParser = .{};
    const rest = burst[u1_ .. req.len * 8];
    const uu = c.feed(rest);
    std.debug.print("C  remainder, fresh parser : used={d} (correct = {d})\n", .{ uu, req.len });
    std.debug.print("   remainder starts with   : <{s}>\n", .{rest[0..@min(rest.len, 24)]});
}
