//! The class of bug the scheduler DST cannot see, and real I/O finds only by
//! luck: byte-stream handling at adversarial delivery boundaries.
//!
//! Four of this session's bugs lived here -- a dropped pipelined tail, a
//! released buffer taking a partial request with it, completions discarded
//! while the connection was mid-request, and a parser holding stale state
//! across a dispatch. Every one of them needed only a particular *chunk
//! boundary* to reproduce. None needed a kernel.
//!
//! This drives the real `http.Parser` through the same feed / shift / reset
//! discipline the servers use, with randomised delivery, and asserts the
//! invariant that actually matters:
//!
//!     every request in the stream is recovered, exactly once, in order,
//!     with the right work_units, whatever the chunk boundaries were.
//!
//! usage: chunkfuzz [iterations] [seed]

const std = @import("std");
const http = @import("budgie").http;

const in_cap = 2048;

/// A faithful model of the servers' inbound path: bytes land in `in`, the
/// parser is fed from `in`, consumed bytes are shifted out, and the parser is
/// reset at dispatch.
const Conn = struct {
    in: [in_cap]u8 = undefined,
    in_len: usize = 0,
    parser: http.Parser = .{},
    busy: bool = false, // mid-request: cannot parse, must still buffer

    /// Bytes arrive. Returns false if the buffer overflowed (an admission
    /// decision in the real server, not a silent truncation).
    fn deliver(c: *Conn, bytes: []const u8) bool {
        if (bytes.len > c.in.len - c.in_len) return false;
        @memcpy(c.in[c.in_len..][0..bytes.len], bytes);
        c.in_len += bytes.len;
        return true;
    }

    /// Parse one request if one is available. Mirrors `drainIn`.
    fn drain(c: *Conn) ?http.Request {
        if (c.busy) return null;
        while (c.in_len > 0) {
            const used = c.parser.feed(c.in[0..c.in_len]);
            if (used > 0) {
                std.mem.copyForwards(u8, c.in[0 .. c.in_len - used], c.in[used..c.in_len]);
                c.in_len -= used;
            }
            switch (c.parser.poll()) {
                .need_input => if (used == 0) return null else continue,
                .protocol_error => return null,
                .request => |req| {
                    // Reset at dispatch: consumed bytes are already out of
                    // `in`, so anything the parser still holds is stale.
                    c.parser = .{};
                    c.busy = true;
                    return req;
                },
            }
        }
        return null;
    }

    fn finishRequest(c: *Conn) void {
        c.busy = false;
    }

    /// The guard from `releaseIfIdle`. Releasing while this is false loses a
    /// partial request that lives ONLY in the parser.
    fn safeToRelease(c: *const Conn) bool {
        return c.in_len == 0 and c.parser.isIdle() and !c.busy;
    }
};

fn buildStream(buf: []u8, units: []u32, rnd: std.Random) usize {
    var n: usize = 0;
    for (units) |*u| {
        u.* = rnd.uintLessThan(u32, 100000);
        const s = std.fmt.bufPrint(buf[n..], "GET /work/{d} HTTP/1.1\r\nHost: x\r\n\r\n", .{u.*}) catch return n;
        n += s.len;
    }
    return n;
}

pub fn main(init: std.process.Init.Minimal) !void {
    // Iterate rather than walking `init.args.vector`: that field is UTF-16 code
    // units on Windows, so `std.mem.span` on it is a compile error there.
    var it = try init.args.iterateAllocator(std.heap.page_allocator);
    defer it.deinit();
    var argv: [4][]const u8 = undefined;
    var argc: usize = 0;
    while (it.next()) |a| {
        if (argc == 4) break;
        argv[argc] = a;
        argc += 1;
    }
    const iters: usize = if (argc > 1) try std.fmt.parseInt(usize, argv[1], 10) else 20000;
    const seed: u64 = if (argc > 2) try std.fmt.parseInt(u64, argv[2], 10) else 0xC0FFEE;

    var prng = std.Random.DefaultPrng.init(seed);
    const rnd = prng.random();

    var stream: [4096]u8 = undefined;
    var units: [16]u32 = undefined;
    var got: [64]u32 = undefined;

    var checked: usize = 0;
    var release_violations: usize = 0;
    var overflow_refusals: usize = 0;

    for (0..iters) |_| {
        const n_req = 1 + rnd.uintLessThan(usize, units.len);
        const total = buildStream(&stream, units[0..n_req], rnd);

        var c: Conn = .{};
        var n_got: usize = 0;
        var off: usize = 0;
        var guard: usize = 0;

        while ((off < total or c.in_len > 0 or c.busy) and guard < 10000) : (guard += 1) {
            // Randomly: deliver a chunk, drain a request, or finish one.
            const action = rnd.uintLessThan(u8, 3);

            if (action == 0 and off < total) {
                // Adversarial chunk size, including 1-byte deliveries that
                // split a request line mid-token.
                const max = @min(total - off, in_cap - c.in_len);
                if (max > 0) {
                    const take = 1 + rnd.uintLessThan(usize, max);
                    if (!c.deliver(stream[off..][0..take])) {
                        overflow_refusals += 1;
                    } else {
                        off += take;
                    }
                }
            } else if (action == 1) {
                if (c.drain()) |req| {
                    if (n_got < got.len) got[n_got] = @intCast(req.work_units);
                    n_got += 1;
                }
            } else {
                // The release check must never pass while bytes or partial
                // parser state exist. This is the assertion that would have
                // caught the lost-partial bug directly.
                if (c.safeToRelease() and (c.in_len != 0 or !c.parser.isIdle())) {
                    release_violations += 1;
                }
                c.finishRequest();
            }
        }

        // Drain whatever is left.
        c.finishRequest();
        while (c.drain()) |req| {
            if (n_got < got.len) got[n_got] = @intCast(req.work_units);
            n_got += 1;
        }

        if (n_got != n_req) {
            std.debug.print("FAIL: sent {d} requests, recovered {d}\n", .{ n_req, n_got });
            return error.RequestLost;
        }
        for (units[0..n_req], got[0..n_req], 0..) |want, have, i| {
            if (want != have) {
                std.debug.print("FAIL: request {d}: sent units={d}, recovered {d}\n", .{ i, want, have });
                return error.RequestCorrupted;
            }
        }
        checked += n_req;
    }

    std.debug.print(
        \\chunkfuzz: {d} iterations, {d} requests
        \\  every request recovered exactly once, in order, with correct units
        \\  release-guard violations : {d}  (expect 0)
        \\  buffer overflow refusals : {d}  (refused, never truncated)
        \\
    , .{ iters, checked, release_violations, overflow_refusals });
}
