const std = @import("std");
const http = @import("coopkernel").http;

// NOTE: the parser must outlive the Event. Request slices point into
// `p.buf`, so `p` is passed in rather than being a local whose frame dies
// before the caller reads the result. The first version of this test got
// that wrong and printed garbage -- the borrow contract is real.
fn drive(p: *http.Parser, input: []const u8, chunk: usize) struct { ev: http.Event, consumed: usize } {
    p.* = .{};
    var off: usize = 0;
    while (off < input.len) {
        const n = @min(chunk, input.len - off);
        const used = p.feed(input[off..][0..n]);
        off += used;
        if (used < n) break;                 // parser stopped at message end
        if (p.poll() != .need_input) break;
    }
    return .{ .ev = p.poll(), .consumed = off };
}

pub fn main() !void {
    const req = "GET /work/4200 HTTP/1.1\r\nHost: x\r\nUser-Agent: z\r\n\r\n";

    // 1. Every split point must give an identical result. This is the test a
    //    fused parser cannot have, because it would need a real socket.
    var wp: http.Parser = .{};
    const whole = drive(&wp, req, req.len);
    var mismatches: usize = 0;
    for (1..req.len + 1) |chunk| {
        var cp: http.Parser = .{};
        const r = drive(&cp, req, chunk);
        const same = switch (r.ev) {
            .request => |q| switch (whole.ev) {
                .request => |w| std.mem.eql(u8, q.method, w.method) and
                    std.mem.eql(u8, q.target, w.target) and q.work_units == w.work_units and
                    r.consumed == whole.consumed,
                else => false,
            },
            else => false,
        };
        if (!same) { mismatches += 1; std.debug.print("  MISMATCH at chunk={d}: {any}\n", .{ chunk, r.ev }); }
    }
    std.debug.print("split-invariance over {d} chunk sizes: {d} mismatches (expect 0)\n", .{ req.len, mismatches });
    switch (whole.ev) {
        .request => |q| std.debug.print("parsed: method={s} target={s} units={d} consumed={d}/{d}\n",
            .{ q.method, q.target, q.work_units, whole.consumed, req.len }),
        else => std.debug.print("UNEXPECTED {any}\n", .{whole.ev}),
    }

    // 2. Pipelining: feed two requests, only the first is consumed.
    const two = req ++ "GET /work/7 HTTP/1.1\r\n\r\n";
    var p: http.Parser = .{};
    const used = p.feed(two);
    std.debug.print("pipelined: consumed={d} (expect {d}), leftover={d} bytes\n",
        .{ used, req.len, two.len - used });
    p.reset();
    const used2 = p.feed(two[used..]);
    switch (p.poll()) {
        .request => |q| std.debug.print("second request after reset: target={s} units={d} consumed={d}\n",
            .{ q.target, q.work_units, used2 }),
        else => |e| std.debug.print("UNEXPECTED {any}\n", .{e}),
    }

    // 3. Malformed and oversized input are events, not crashes.
    const bad = [_][]const u8{
        "GARBAGE\r\n\r\n",
        " /x HTTP/1.1\r\n\r\n",
        "GET noslash HTTP/1.1\r\n\r\n",
        "\r\n\r\n",
    };
    for (bad, 0..) |b, i| {
        var q: http.Parser = .{};
        _ = q.feed(b);
        std.debug.print("malformed input #{d: <2} -> {any}\n", .{ i, q.poll() });
    }
    var big: http.Parser = .{};
    var junk: [http.max_request_bytes + 64]u8 = @splat('A');
    _ = big.feed(&junk);
    std.debug.print("oversized -> {any}\n", .{big.poll()});

    // 4. Fuzz-shaped: random bytes never crash, never exceed the buffer.
    var prng = std.Random.DefaultPrng.init(0xC0FFEE);
    const rnd = prng.random();
    var buf: [4096]u8 = undefined;
    for (0..20000) |_| {
        const n = rnd.uintLessThan(usize, buf.len);
        rnd.bytes(buf[0..n]);
        var f: http.Parser = .{};
        var o: usize = 0;
        while (o < n) {
            const step = @max(1, rnd.uintLessThan(usize, 64));
            const c = f.feed(buf[o..@min(n, o + step)]);
            if (c == 0) break;
            o += c;
            if (f.poll() != .need_input) break;
        }
        std.debug.assert(f.bytesBuffered() <= http.max_request_bytes);
    }
    std.debug.print("20000 random inputs: no crash, buffer bound held\n", .{});
}
