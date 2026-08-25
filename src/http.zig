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
    err: Error = .bad_request_line,
    req: Request = .{ .method = &.{}, .target = &.{}, .work_units = 0 },

    pub fn reset(p: *Parser) void {
        p.state = .accumulating;
        p.len = 0;
    }

    /// Returns how many bytes were consumed. Stops at the end of the header
    /// block so a pipelined follow-up request is left for the caller.
    /// Consume bytes up to and including the end of ONE message, and no
    /// further. Returns how many bytes of `bytes` were used.
    ///
    /// The previous version copied the whole slice into `p.buf` and returned
    /// `header_len - before`. With a pipelined client that meant `p.buf` held
    /// bytes belonging to LATER requests, so `before` stopped describing a
    /// prefix of the current message and the arithmetic silently produced a
    /// short consume -- the caller then re-fed bytes the parser had already
    /// taken and the request line parsed as garbage. Stopping at the
    /// terminator makes `p.len` hold exactly one header, always.
    pub fn feed(p: *Parser, bytes: []const u8) usize {
        if (p.state != .accumulating) return 0;
        var i: usize = 0;
        while (i < bytes.len) : (i += 1) {
            if (p.len == max_request_bytes) {
                p.fail(.too_long);
                return i;
            }
            p.buf[p.len] = bytes[i];
            p.len += 1;
            if (p.len >= 4 and std.mem.eql(u8, p.buf[p.len - 4 .. p.len], "\r\n\r\n")) {
                p.parseRequestLine(p.buf[0 .. p.len - 4]);
                return i + 1;
            }
        }
        return bytes.len;
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

    /// True only when the parser holds nothing. A parser mid-request owns
    /// bytes that exist nowhere else -- releasing its buffer loses them.
    pub fn debugBuf(p: *const Parser) []const u8 {
        return p.buf[0..@min(p.len, 64)];
    }

    pub fn isIdle(p: *const Parser) bool {
        return p.state == .accumulating and p.len == 0;
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
