//! A blocking HTTP client, for tests that drive a real server over loopback.
//!
//! Shared by `echo_test.zig` and `server_test.zig`. It uses `budgie.sys` for
//! the socket calls, which is the same shim the servers use, so a platform
//! break shows up in the test rather than only in production.
//!
//! Everything here retries on `EINTR`. `app/server.zig` raises `SIGALRM` on a
//! timer, and a signal landing between `poll` and `read` would otherwise look
//! like a closed connection and fail a test that had nothing wrong with it.

const std = @import("std");
const budgie = @import("budgie");
const sys = budgie.sys;

pub const ok_head = "HTTP/1.1 200 OK";

/// How many times to retry a syscall that failed without meaning it.
const eintr_retries = 8;

/// Connect attempts, each on a fresh descriptor.
const connect_retries = 40;

/// Call before touching a socket. Without it a write to a peer that has gone
/// away kills the test process, which reads as a hang rather than a failure.
pub fn ignoreSigpipe() void {
    budgie.sys.ignoreSigpipe();
}

pub const Client = struct {
    fd: i32,

    /// Connect, retrying on a fresh descriptor each time.
    ///
    /// A socket whose connect failed cannot be reused; a second connect on it
    /// returns EALREADY or EISCONN forever. Retrying on the same descriptor
    /// turns one transient refusal under a burst into a permanent failure,
    /// which is what made this flaky before.
    pub fn connect(port: u16) !Client {
        var attempt: usize = 0;
        while (attempt < connect_retries) : (attempt += 1) {
            const rc = sys.tcpSocket();
            if (sys.sysErr(rc)) {
                sleepMs(5);
                continue;
            }
            const fd: i32 = @intCast(rc);
            var addr = sys.SockAddrIn{ .port = sys.hostToNetPort(port), .addr = sys.loopback };
            if (!sys.sysErr(sys.connect(fd, &addr))) return .{ .fd = fd };
            sys.close(fd);
            sleepMs(5); // a full accept queue drains; give it a moment
        }
        return error.ConnectFailed;
    }

    pub fn send(c: Client, bytes: []const u8) !void {
        var off: usize = 0;
        var stalls: usize = 0;
        while (off < bytes.len) {
            const rc = sys.write(c.fd, bytes[off..].ptr, bytes.len - off);
            if (sys.sysErr(rc)) {
                stalls += 1;
                if (stalls > eintr_retries) return error.WriteFailed;
                continue;
            }
            if (rc == 0) return error.WriteZero;
            off += rc;
            stalls = 0;
        }
    }

    /// Read until `want` bytes arrive, the peer closes, or the timeout expires.
    /// Returns what actually came, so a caller can tell a short answer from no
    /// answer. Never hangs, so a stalled server fails one check rather than the
    /// whole run.
    pub fn recv(c: Client, buf: []u8, want: usize, timeout_ms: i32) usize {
        var got: usize = 0;
        var stalls: usize = 0;
        while (got < want and got < buf.len) {
            var p = [_]std.posix.pollfd{.{ .fd = c.fd, .events = std.posix.POLL.IN, .revents = 0 }};
            const ready = std.posix.poll(&p, timeout_ms) catch return got;
            if (ready == 0) return got; // timed out

            const rc = sys.read(c.fd, buf[got..].ptr, buf.len - got);
            if (sys.sysErr(rc)) {
                stalls += 1;
                if (stalls > eintr_retries) return got;
                continue;
            }
            if (rc == 0) return got; // peer closed
            got += rc;
            stalls = 0;
        }
        return got;
    }

    /// Read until `needle` has appeared `count` times, the peer closes, or the
    /// timeout expires. Returns how many bytes arrived.
    ///
    /// Waiting on a marker rather than a byte count matters more than it
    /// looks. A response here is 63 bytes; asking `recv` for 64 blocks for the
    /// whole timeout on every single request, which turns a fast test into a
    /// slow one and hides real stalls behind expected ones.
    pub fn recvUntil(c: Client, buf: []u8, needle: []const u8, count: usize, timeout_ms: i32) usize {
        var got: usize = 0;
        var stalls: usize = 0;
        while (got < buf.len) {
            if (countOf(buf[0..got], needle) >= count) return got;

            var p = [_]std.posix.pollfd{.{ .fd = c.fd, .events = std.posix.POLL.IN, .revents = 0 }};
            const ready = std.posix.poll(&p, timeout_ms) catch return got;
            if (ready == 0) return got;

            const rc = sys.read(c.fd, buf[got..].ptr, buf.len - got);
            if (sys.sysErr(rc)) {
                stalls += 1;
                if (stalls > eintr_retries) return got;
                continue;
            }
            if (rc == 0) return got;
            got += rc;
            stalls = 0;
        }
        return got;
    }

    /// Whether the peer closed within the timeout. Tells "closed" apart from
    /// "idle", which matters when the expected behaviour is a refusal.
    pub fn closedByPeer(c: Client, timeout_ms: i32) bool {
        var p = [_]std.posix.pollfd{.{ .fd = c.fd, .events = std.posix.POLL.IN, .revents = 0 }};
        const ready = std.posix.poll(&p, timeout_ms) catch return false;
        if (ready == 0) return false;
        var b: [1]u8 = undefined;
        const rc = sys.read(c.fd, &b, 1);
        return !sys.sysErr(rc) and rc == 0;
    }

    /// Make `close` send a RST rather than a FIN, so the peer's next write
    /// lands on a torn-down connection. This is what a client crashing or a
    /// load balancer giving up looks like from the server's side, and it is
    /// the case that used to kill the process outright.
    pub fn resetOnClose(c: Client) void {
        const linger = extern struct { onoff: c_int, seconds: c_int }{ .onoff = 1, .seconds = 0 };
        _ = budgie.sys.setLinger(c.fd, @ptrCast(&linger), @sizeOf(@TypeOf(linger)));
    }

    /// Shrink this end's receive buffer so the server's writes fill it and
    /// start returning EAGAIN. Call before sending.
    pub fn throttleReceive(c: Client, bytes: i32) void {
        budgie.sys.setRecvBuf(c.fd, bytes);
    }

    /// Stop blocking in `send`, so a test can push until the server stops
    /// draining and then say how far it got. A blocking send against a server
    /// that has parked on its own write does not fail, it hangs.
    pub fn setNonblocking(c: Client) void {
        budgie.sys.setNonblock(c.fd);
    }

    /// Write what the kernel will take right now and return how much that was.
    /// Zero means the connection is backed up, which is the state the caller
    /// is usually trying to reach.
    pub fn sendSome(c: Client, bytes: []const u8) usize {
        var off: usize = 0;
        while (off < bytes.len) {
            const rc = sys.write(c.fd, bytes[off..].ptr, bytes.len - off);
            if (sys.sysErr(rc)) return off;
            if (rc == 0) return off;
            off += rc;
        }
        return off;
    }

    pub fn halfClose(c: Client) void {
        _ = sys.shutdown(c.fd, 1); // SHUT_WR
    }

    pub fn close(c: Client) void {
        sys.close(c.fd);
    }
};

/// The status line only, which is what a status assertion must look at.
///
/// Searching the whole response for "408" passes even when the status line
/// says 503, because this server's bodies repeat the code: the body for a
/// deadline is literally "408 deadline missed". A test written that way
/// reports success for a server answering the wrong thing.
pub fn statusLine(bytes: []const u8) []const u8 {
    const eol = std.mem.indexOf(u8, bytes, "\r\n") orelse bytes.len;
    return bytes[0..eol];
}

pub fn statusIs(bytes: []const u8, code: []const u8) bool {
    const line = statusLine(bytes);
    return std.mem.startsWith(u8, line, "HTTP/1.1 ") and
        std.mem.indexOf(u8, line, code) != null;
}

pub fn request(target: []const u8, buf: []u8) []const u8 {
    // `@panic` and not `catch unreachable`. The buffer belongs to the caller,
    // so "this cannot fail" is a claim about somebody else's argument, and
    // `unreachable` is undefined behaviour in a release build and an
    // unexplained panic in a safe one. This says which buffer was too small.
    return std.fmt.bufPrint(buf, "GET {s} HTTP/1.1\r\nHost: x\r\n\r\n", .{target}) catch
        @panic("httpclient.request: target buffer too small");
}

/// Bytes in one `workRequest`. Fixed, so a caller that only managed a partial
/// send can divide and know how many whole requests the server received.
pub const work_request_bytes = "GET /work/0000 HTTP/1.1\r\nHost: x\r\n\r\n".len;

/// A work request with the unit count zero-padded, so every request in a burst
/// is the same length.
///
/// Takes a pointer to an array of exactly the right size rather than a slice,
/// which is the strongest of the options for a call that "cannot fail": the
/// buffer being too small stops being a runtime panic and becomes a compile
/// error at the call site. `@panic` is what to reach for when the size is not
/// comptime-known, as in `request` above, where the target is a runtime slice
/// and the output length is not knowable here.
///
/// The output length is fixed because the units are zero-padded to four
/// digits, which is the whole reason `work_request_bytes` can be a constant.
pub fn workRequest(units: usize, buf: *[work_request_bytes]u8) []const u8 {
    return std.fmt.bufPrint(buf, "GET /work/{d:0>4} HTTP/1.1\r\nHost: x\r\n\r\n", .{units}) catch
        @panic("httpclient.workRequest: work_request_bytes is wrong");
}

/// How many 200 responses are in a buffer. Counting rather than parsing,
/// because a pipelined burst arrives as one run of bytes.
pub fn countOk(bytes: []const u8) usize {
    return countOf(bytes, ok_head);
}

pub fn countOf(bytes: []const u8, needle: []const u8) usize {
    var n: usize = 0;
    var i: usize = 0;
    while (std.mem.indexOfPos(u8, bytes, i, needle)) |at| : (i = at + needle.len) n += 1;
    return n;
}

/// Sleep, without reaching for `std.Thread.sleep`, which 0.16 moved behind
/// `std.Io`. `poll` on an empty set is a timeout and nothing else.
pub fn sleepMs(ms: i32) void {
    budgie.sys.sleepMs(@intCast(@max(0, ms)));
}

/// Wait for a condition, up to a limit. Returns whether it came true.
///
/// A fixed sleep before reading counters races whatever is still finishing,
/// and the failure it produces is off by one and irreproducible. Polling still
/// catches a real leak, which never balances however long you wait.
pub fn waitUntil(comptime pred: fn () bool, timeout_ms: usize) bool {
    var waited: usize = 0;
    while (waited < timeout_ms) : (waited += 20) {
        if (pred()) return true;
        sleepMs(20);
    }
    return pred();
}

// ------------------------------------------------------------- reporting

pub var failures: usize = 0;
pub var checks: usize = 0;

pub fn check(ok: bool, comptime name: []const u8, detail: anytype) void {
    checks += 1;
    if (ok) {
        std.debug.print("  ok    {s}\n", .{name});
    } else {
        failures += 1;
        std.debug.print("  FAIL  {s}  {any}\n", .{ name, detail });
    }
}

pub fn report() void {
    std.debug.print("\n{d} checks, {d} failures\n", .{ checks, failures });
    if (failures != 0) std.process.exit(1);
}
