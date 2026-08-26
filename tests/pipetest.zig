const std = @import("std");
const http = @import("budgie").http;

// Reproduce exactly what onData does: feed a concatenated burst, parse one
// request, stash the tail, reset, repeat.
pub fn main() !void {
    const req = "GET /work/0 HTTP/1.1\r\nHost: x\r\n\r\n";
    var burst: [4096]u8 = undefined;
    const depth = 8;
    for (0..depth) |i| @memcpy(burst[i * req.len ..][0..req.len], req);
    const total = req.len * depth;
    std.debug.print("burst = {d} requests, {d} bytes, req.len={d}\n", .{ depth, total, req.len });

    var stash: [2048]u8 = undefined;
    var stash_len: usize = total;
    @memcpy(stash[0..total], burst[0..total]);

    var round: u32 = 0;
    while (stash_len > 0 and round < 20) : (round += 1) {
        var p: http.Parser = .{};            // fresh, as after b.* = .{}
        var off: usize = 0;
        var outcome: []const u8 = "none";
        while (off < stash_len) {
            const used = p.feed(stash[off..stash_len]);
            if (used == 0 and p.poll() == .need_input) { outcome = "stalled"; break; }
            off += used;
            switch (p.poll()) {
                .need_input => continue,
                .protocol_error => |e| { outcome = @tagName(e); break; },
                .request => { outcome = "request"; break; },
            }
        }
        const rest = stash_len - off;
        std.debug.print("  round {d:>2}: in={d:>4}B used={d:>3} -> {s:<18} tail={d}B\n",
            .{ round, stash_len, off, outcome, rest });
        if (!std.mem.eql(u8, outcome, "request")) break;
        var tmp: [2048]u8 = undefined;
        @memcpy(tmp[0..rest], stash[off .. off + rest]);
        @memcpy(stash[0..rest], tmp[0..rest]);
        stash_len = rest;
    }
}
