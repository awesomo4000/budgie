//! Minimal reproduction attempt for the keep-alive stall.
//!
//! One multishot recv on a loopback socket, with the same ring setup the
//! server uses: DEFER_TASKRUN, SINGLE_ISSUER, and a provided buffer ring. A
//! sender thread writes two messages 400ms apart. The loop polls exactly the
//! way reactor_uring2 does, on a 4ms timeout SQE, and prints when each
//! completion actually arrives.
//!
//! If message two shows up ~400ms after message one, the kernel and the
//! wrapper are fine and the fault is in how the reactor drives them. If it
//! shows up late, it reproduces outside the server and the search narrows to
//! the ring setup itself.

const std = @import("std");
const linux = std.os.linux;
const IoUring = linux.IoUring;

const buf_group: u16 = 1;
const n_bufs: u16 = 16;
const buf_size: u32 = 2048;
const tag_timeout: u64 = std.math.maxInt(u64);

fn nowMs() i64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, @intCast(ts.sec)) * 1000 + @divTrunc(@as(i64, @intCast(ts.nsec)), 1_000_000);
}

var send_fd: i32 = -1;
var t0: i64 = 0;

fn sender() void {
    // First message immediately, second after a pause the server would spend idle.
    _ = linux.write(send_fd, "AAAA", 4);
    std.debug.print("[{d:>5}ms] sent message 1\n", .{nowMs() - t0});
    var ts: linux.timespec = .{ .sec = 0, .nsec = 400 * 1_000_000 };
    _ = linux.nanosleep(&ts, null);
    _ = linux.write(send_fd, "BBBB", 4);
    std.debug.print("[{d:>5}ms] sent message 2\n", .{nowMs() - t0});
}

pub fn main() !void {
    var fds: [2]i32 = undefined;
    if (linux.socketpair(linux.AF.UNIX, linux.SOCK.STREAM, 0, &fds) != 0) return error.SocketPair;
    const recv_fd = fds[0];
    send_fd = fds[1];

    // Same flags as the server, with the same fallback.
    const flags: u32 = linux.IORING_SETUP_SINGLE_ISSUER | linux.IORING_SETUP_DEFER_TASKRUN;
    var ring = IoUring.init(256, flags) catch try IoUring.init(256, 0);
    defer ring.deinit();
    std.debug.print("ring flags=0x{x}\n", .{ring.flags});

    // Provided buffer ring, via the same workaround the server uses.
    const br = try @import("budgie").uring_bufring.setup(ring.fd, n_bufs, buf_group);
    IoUring.buf_ring_init(br);
    const mask = IoUring.buf_ring_mask(n_bufs);
    const mem = try std.heap.page_allocator.alloc(u8, @as(usize, buf_size) * n_bufs);
    var i: u16 = 0;
    while (i < n_bufs) : (i += 1) {
        const off = @as(usize, buf_size) * i;
        IoUring.buf_ring_add(br, mem[off .. off + buf_size], i, mask, i);
    }
    IoUring.buf_ring_advance(br, n_bufs);

    // Arm one multishot recv, exactly once.
    const sqe = try ring.get_sqe();
    sqe.prep_rw(.RECV, recv_fd, 0, 0, 0);
    sqe.rw_flags = 0;
    sqe.flags |= linux.IOSQE_BUFFER_SELECT;
    sqe.buf_index = buf_group;
    sqe.ioprio |= linux.IORING_RECV_MULTISHOT;
    sqe.user_data = 42;
    _ = try ring.submit();
    std.debug.print("multishot recv armed\n\n", .{});

    t0 = nowMs();
    var thread = try std.Thread.spawn(.{}, sender, .{});
    thread.detach();

    var got: usize = 0;
    var ticks: usize = 0;
    var tick_ts: linux.kernel_timespec = .{ .sec = 0, .nsec = 4_000_000 };
    var tick_armed = false;

    while (nowMs() - t0 < 2000) {
        if (!tick_armed) {
            _ = ring.timeout(tag_timeout, &tick_ts, 0, 0) catch {};
            tick_armed = true;
        }
        _ = ring.submit_and_wait(1) catch break;

        var cqes: [32]linux.io_uring_cqe = undefined;
        const n = ring.copy_cqes(&cqes, 0) catch break;
        for (cqes[0..n]) |cqe| {
            if (cqe.user_data == tag_timeout) {
                tick_armed = false;
                ticks += 1;
                continue;
            }
            const more = cqe.flags & linux.IORING_CQE_F_MORE != 0;
            got += 1;
            std.debug.print("[{d:>5}ms] CQE res={d} more={} (after {d} ticks)\n", .{ nowMs() - t0, cqe.res, more, ticks });
            ticks = 0;
        }
    }

    std.debug.print("\ncompletions seen: {d} (expect 2)\n", .{got});
    if (got < 2) std.debug.print("REPRODUCED: the second message never surfaced\n", .{});
}
