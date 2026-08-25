//! A sans-I/O HTTP request-line parser.
//!
//! No fd, no allocator, no suspension point, no error union carrying a runtime
//! concern. It is a pure function of (state, input) -> (state, event), which
//! means it needs no budget, no cancellation handling, and no effects: all
//! three of those are driver concerns and stay in server.zig.
//!
//! Contract:
//!
//!     feed(bytes) -> consumed   never blocks, may consume less than given
//!     poll()      -> Event      pure, no side effects
//!     reset()                   for keep-alive
//!
//! `feed` consuming less than it was given is the normal case, not an error:
//! it stops at the end of a message so the caller keeps the pipelined
//! remainder. That is the property a pull-based reader hides and then gets
//! wrong.
//!
//! Because nothing here performs I/O, byte-at-a-time input must produce
//! results identical to whole-message input. `parser_test.zig` asserts that
//! over every split point, which is the test a fused parser cannot have.

const std = @import("std");

pub const max_request_bytes = 512;

pub const Error = enum {
    too_long,
    bad_request_line,
    bad_method,
};

pub const Request = struct {
    method: []const u8,
    target: []const u8,
    /// Parsed out of "/work/<N>" for this toy protocol. 0 when absent.
    work_units: i64,
};

pub const Event = union(enum) {
    /// Nothing decided yet; feed more bytes.
    need_input,
    /// A complete request. Slices point into the parser's own buffer and stay
    /// valid until the next `reset`.
    request: Request,
    /// Terminal. The driver decides what status to write; the parser does not
    /// know what an HTTP response is.
    protocol_error: Error,
};

const State = enum { accumulating, complete, failed };

pub const Parser = struct {
    state: State = .accumulating,
    buf: [max_request_bytes]u8 = undefined,
    len: usize = 0,
    /// Offset already scanned for the header terminator, so re-scanning after
    /// a partial feed is O(new bytes) rather than O(total).
    scanned: usize = 0,
    err: Error = .bad_request_line,
    req: Request = .{ .method = &.{}, .target = &.{}, .work_units = 0 },

    pub fn reset(p: *Parser) void {
        p.state = .accumulating;
        p.len = 0;
        p.scanned = 0;
    }

    /// Returns how many bytes were consumed. Stops at the end of the header
    /// block so a pipelined follow-up request is left for the caller.
    pub fn feed(p: *Parser, bytes: []const u8) usize {
        if (p.state != .accumulating) return 0;

        const room = max_request_bytes - p.len;
        const take = @min(room, bytes.len);
        @memcpy(p.buf[p.len..][0..take], bytes[0..take]);
        const before = p.len;
        p.len += take;

        // Rescan from a little behind the previous end so a terminator split
        // across two feeds is still found.
        const from = if (p.scanned >= 3) p.scanned - 3 else 0;
        if (std.mem.indexOfPos(u8, p.buf[0..p.len], from, "\r\n\r\n")) |end| {
            const header_len = end + 4;
            p.parseRequestLine(p.buf[0..end]);
            // Consume only up to the end of this message.
            return header_len - before;
        }
        p.scanned = p.len;

        if (take < bytes.len) {
            // Buffer full and still no terminator.
            p.state = .failed;
            p.err = .too_long;
        }
        return take;
    }

    pub fn poll(p: *const Parser) Event {
        return switch (p.state) {
            .accumulating => .need_input,
            .complete => .{ .request = p.req },
            .failed => .{ .protocol_error = p.err },
        };
    }

    /// Bytes currently buffered — the honest quantity to charge parse work
    /// against, rather than a made-up constant.
    pub fn bytesBuffered(p: *const Parser) usize {
        return p.len;
    }

    fn parseRequestLine(p: *Parser, header_block: []const u8) void {
        const eol = std.mem.indexOf(u8, header_block, "\r\n") orelse header_block.len;
        const line = header_block[0..eol];

        const sp1 = std.mem.indexOfScalar(u8, line, ' ') orelse return p.fail(.bad_request_line);
        const method = line[0..sp1];
        if (method.len == 0) return p.fail(.bad_method);

        const rest = line[sp1 + 1 ..];
        const sp2 = std.mem.indexOfScalar(u8, rest, ' ') orelse return p.fail(.bad_request_line);
        const target = rest[0..sp2];
        if (target.len == 0 or target[0] != '/') return p.fail(.bad_request_line);

        p.req = .{ .method = method, .target = target, .work_units = workUnits(target) };
        p.state = .complete;
    }

    fn fail(p: *Parser, e: Error) void {
        p.state = .failed;
        p.err = e;
    }

    fn workUnits(target: []const u8) i64 {
        const prefix = "/work/";
        if (!std.mem.startsWith(u8, target, prefix)) return 0;
        var end = prefix.len;
        while (end < target.len and target[end] >= '0' and target[end] <= '9') end += 1;
        if (end == prefix.len) return 0;
        return std.fmt.parseInt(i64, target[prefix.len..end], 10) catch 0;
    }
};
