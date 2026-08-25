//! The reactor, in COMPLETION mode. This one cannot keep the old interface,
//! and that is the point.
//!
//! epoll and POLL_ADD both answer "this fd is ready" and the application then
//! calls `read`. Here the kernel does the read and hands back bytes, so:
//!
//!     old:  watch(task, fd, .read)  -> makeRunnable -> app calls read()
//!     new:  armRecv(task, fd)       -> completions carrying (task, bytes)
//!
//! `wait` no longer reports readiness; it fills a completion queue the caller
//! drains. The application never calls `read` again.
//!
//! Two kernel features do the work:
//!
//!   IORING_RECV_MULTISHOT   one submission per connection FOR ITS WHOLE LIFE.
//!                           Completions keep arriving. No re-arm, so the
//!                           epoll_ctl-per-park cost is gone as well as the
//!                           read-per-request cost.
//!
//!   provided buffer ring    the kernel picks a buffer AT COMPLETION TIME from
//!                           a shared pool. A connection with no data pending
//!                           holds no read buffer at all -- the same idea as
//!                           the userspace IoBuf pool, one level down and
//!                           enforced by the kernel, which returns -ENOBUFS
//!                           as a ready-made admission signal.
//!
//! Writes stay as ordinary `write` syscalls here: making them completions too
//! means tracking in-flight send buffers, which is a separate change. Write
//! readiness (rare) still uses poll_add.

const std = @import("std");
const linux = std.os.linux;
const IoUring = linux.IoUring;
const sched = @import("sched.zig");

pub const Interest = enum { read, write };

const queue_depth: u16 = 4096;
const max_cqes = 512;
const buf_group: u16 = 1;

/// Tags in user_data so a completion's origin is unambiguous.
const tag_recv: u64 = 0;
const tag_poll: u64 = 1 << 40;
const tag_timeout: u64 = 1 << 41;
const tag_mask: u64 = 0xffff_ffff;

pub const Kind = enum { data, eof, err, writable };

pub const Completion = struct {
    task: sched.TaskId,
    kind: Kind,
    data: []const u8 = &.{},
    /// Held so the buffer can be returned to the ring after the app consumes it.
    cqe: linux.io_uring_cqe = undefined,
    has_buf: bool = false,
};

pub const Reactor = struct {
    ring: IoUring = undefined,
    group: IoUring.BufferGroup = undefined,
    armed_recv: [sched.max_tasks]bool = @splat(false),
    armed_poll: [sched.max_tasks]bool = @splat(false),
    armed: usize = 0,

    done: [max_cqes]Completion = undefined,
    done_n: usize = 0,
    done_i: usize = 0,

    waits: u64 = 0,
    fds_polled: u64 = 0,
    enters: u64 = 0,
    sqes_total: u64 = 0,
    recv_arms: u64 = 0,
    multishot_reups: u64 = 0,
    enobufs: u64 = 0,
    bytes_in: u64 = 0,

    pub fn init(r: *Reactor, n_bufs: u16, buf_size: u32) !void {
        r.ring = try IoUring.init(queue_depth, 0);
        r.group = try IoUring.BufferGroup.init(&r.ring, std.heap.page_allocator, buf_group, buf_size, n_bufs);
    }

    /// One submission for the connection's entire life.
    pub fn armRecv(r: *Reactor, task: sched.TaskId, fd: i32) void {
        _ = r.group.recv_multishot(tag_recv | @as(u64, task), fd, 0) catch {
            _ = r.ring.submit() catch return;
            r.enters += 1;
            _ = r.group.recv_multishot(tag_recv | @as(u64, task), fd, 0) catch return;
        };
        r.sqes_total += 1;
        r.recv_arms += 1;
        if (!r.armed_recv[task]) {
            r.armed_recv[task] = true;
            r.armed += 1;
        }
    }

    /// Readiness path, still used for the listener sockets and the control
    /// surface -- a listening fd cannot take a multishot recv, and the control
    /// surface is deliberately trivial. Data connections do not come here.
    pub fn watch(r: *Reactor, task: sched.TaskId, fd: i32, i: Interest) void {
        const mask: u32 = switch (i) {
            .read => linux.POLL.IN,
            .write => linux.POLL.OUT,
        };
        _ = r.ring.poll_add(tag_poll | @as(u64, task), fd, mask) catch return;
        r.sqes_total += 1;
        r.armed_poll[task] = true;
    }

    pub fn unwatch(r: *Reactor, task: sched.TaskId) void {
        if (r.armed_recv[task]) {
            _ = r.ring.poll_remove(tag_timeout, tag_recv | @as(u64, task)) catch {};
            r.armed_recv[task] = false;
            if (r.armed > 0) r.armed -= 1;
        }
        r.armed_poll[task] = false;
    }

    pub fn watching(r: *const Reactor, task: sched.TaskId) bool {
        return r.armed_recv[task];
    }

    /// Drain one completion. The returned `data` slice points into the kernel
    /// buffer ring and is valid until `release` is called for it.
    pub fn next(r: *Reactor) ?Completion {
        if (r.done_i >= r.done_n) return null;
        const c = r.done[r.done_i];
        r.done_i += 1;
        return c;
    }

    pub fn release(r: *Reactor, c: Completion) void {
        if (!c.has_buf) return;
        r.group.put(c.cqe) catch {};
    }

    pub fn wait(r: *Reactor, s: *sched.Sched, timeout_ms: i32) usize {
        r.waits += 1;
        r.fds_polled += r.armed;
        r.done_n = 0;
        r.done_i = 0;

        var ts: linux.kernel_timespec = .{
            .sec = @divTrunc(timeout_ms, 1000),
            .nsec = @as(i64, @intCast(@mod(timeout_ms, 1000))) * 1_000_000,
        };
        if (timeout_ms > 0) {
            _ = r.ring.timeout(tag_timeout, &ts, 0, 0) catch {};
            r.sqes_total += 1;
        }

        const wait_nr: u32 = if (timeout_ms == 0) 0 else 1;
        _ = r.ring.submit_and_wait(wait_nr) catch return 0;
        r.enters += 1;

        var cqes: [max_cqes]linux.io_uring_cqe = undefined;
        const n = r.ring.copy_cqes(&cqes, 0) catch return 0;
        for (cqes[0..n]) |cqe| {
            if (cqe.user_data == tag_timeout) continue;
            const task: sched.TaskId = @intCast(cqe.user_data & tag_mask);

            if (cqe.user_data & tag_poll != 0) {
                // Readiness, as before: the task wakes and does its own I/O.
                r.armed_poll[task] = false;
                s.makeRunnable(task, .io);
                continue;
            }

            // recv completion
            const more = cqe.flags & linux.IORING_CQE_F_MORE != 0;
            if (!more and r.armed_recv[task]) {
                // Multishot ended -- must be re-armed by the app on its next
                // park. Track it so the cost is visible rather than assumed.
                r.armed_recv[task] = false;
                if (r.armed > 0) r.armed -= 1;
                r.multishot_reups += 1;
            }
            if (cqe.res == -@as(i32, @intCast(@intFromEnum(linux.E.NOBUFS)))) {
                r.enobufs += 1;
                continue;
            }
            if (cqe.res < 0) {
                r.push(.{ .task = task, .kind = .err });
                s.makeRunnable(task, .io);
                continue;
            }
            if (cqe.res == 0) {
                r.push(.{ .task = task, .kind = .eof });
                s.makeRunnable(task, .io);
                continue;
            }
            const data = r.group.get(cqe) catch {
                r.push(.{ .task = task, .kind = .err });
                s.makeRunnable(task, .io);
                continue;
            };
            r.bytes_in += data.len;
            r.push(.{ .task = task, .kind = .data, .data = data, .cqe = cqe, .has_buf = true });
        }
        return r.done_n;
    }

    fn push(r: *Reactor, c: Completion) void {
        if (r.done_n == r.done.len) return;
        r.done[r.done_n] = c;
        r.done_n += 1;
    }
};
