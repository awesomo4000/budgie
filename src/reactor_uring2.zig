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

fn nowNsLocal() i64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, @intCast(ts.sec)) * 1_000_000_000 + @as(i64, @intCast(ts.nsec));
}
pub var defer_taskrun: bool = true;
pub var coop_taskrun: bool = false;

/// Tags in user_data so a completion's origin is unambiguous.
const tag_recv: u64 = 0;
const tag_poll: u64 = 1 << 40;
const tag_timeout: u64 = 1 << 41;
const tag_send: u64 = 1 << 42;
const tag_mask: u64 = 0xffff_ffff;

/// Completion keys carry a GENERATION, not just a task id.
///
/// A completion can be queued in the CQ before the task that owns it is
/// released -- a cancelled or closed connection still produces one. If the
/// slot is reused before that completion is reaped, its bytes are delivered to
/// whoever inherited the slot. Same failure as the buffer-ring aliasing bug,
/// just rarer because it needs a close/reuse race.
///
/// On IOCP this stops being rare: CancelIoEx does not complete synchronously,
/// and a cancelled WSARecv still posts a completion with
/// ERROR_OPERATION_ABORTED. Late completions are the normal shutdown path
/// there, so the key MUST be generational for that port to be correct.
const gen_shift: u6 = 20; // task ids are < 2^20; generation gets the rest
fn key(tag: u64, task: sched.TaskId, gen: u32) u64 {
    return tag | (@as(u64, gen) << gen_shift) | @as(u64, task);
}
pub var use_gen: bool = true;
fn keyTask(ud: u64) sched.TaskId {
    return @intCast(ud & ((1 << gen_shift) - 1));
}
fn keyGen(ud: u64) u32 {
    return @intCast((ud & tag_mask) >> gen_shift);
}

pub const Kind = enum { data, eof, err, writable, sent };

pub const Completion = struct {
    task: sched.TaskId,
    kind: Kind,
    data: []const u8 = &.{},
    /// Held so the buffer can be returned to the ring after the app consumes it.
    cqe: linux.io_uring_cqe = undefined,
    has_buf: bool = false,
    /// Ring buffer id backing `data`. The app returns it with `release` when
    /// it has consumed the bytes -- or NOT, if it has no room, which is how
    /// backpressure is expressed.
    buf_id: u16 = 0,
    /// Bytes actually sent, for `.sent` completions.
    sent: i32 = 0,
};

pub const Reactor = struct {
    ring: IoUring = undefined,
    /// Own buffer ring rather than std's `BufferGroup`, which hardcodes
    /// INCREMENTAL consumption (`.inc = true`). In that mode several
    /// completions share one buffer id with a head offset maintained in
    /// userspace, and getting that bookkeeping even slightly wrong shifts
    /// slice boundaries -- we measured one task receiving another task's
    /// 16-byte prefix. Non-incremental gives each completion a whole distinct
    /// buffer, so a completion's bytes are exactly `buf[id][0..res]` with no
    /// shared state to get wrong.
    br: *align(4096) linux.io_uring_buf_ring = undefined,
    bufs_mem: []u8 = &.{},
    buf_size: u32 = 0,
    n_bufs: u16 = 0,
    br_mask: u16 = 0,
    armed_recv: [sched.max_tasks]bool = @splat(false),
    armed_poll: [sched.max_tasks]bool = @splat(false),
    armed: usize = 0,

    /// Completion data is copied out of the ring immediately and the buffer
    /// returned in the same breath.
    ///
    /// The buffer group runs in INCREMENTAL consumption mode, where several
    /// completions can share one buffer id with a head offset that only moves
    /// when `put` is called. Collecting every `get` during the wait and
    /// deferring every `put` to the drain loop therefore computed later slices
    /// from a stale head -- data arrived starting mid-request and the parser
    /// rejected it. get-copy-put per completion removes the aliasing entirely,
    /// at one memcpy that `parser.feed` was going to pay anyway.
    stage: [64 * 1024]u8 = undefined,
    stage_n: usize = 0,
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
    cqes_total: u64 = 0,
    /// Completions to wait for. With DEFER_TASKRUN the kernel has already
    /// accumulated a batch by the time it wakes us, so waking on the FIRST and
    /// then reaping everything available beats blocking for a target count --
    /// a target of N stalls whenever fewer than N are in flight.
    batch_target: u32 = 1,
    /// A timeout SQE submitted on every wait doubles ring traffic: an extra
    /// SQE in and an extra CQE out per syscall. One periodic timeout, re-armed
    /// only when it fires, gives the timer wheel its granularity for a fixed
    /// cost instead of a per-wait one.
    tick_armed: bool = false,
    tick_ns: i64 = 4_000_000,
    /// Adaptive batching window. Wait for `batch_target` completions OR this
    /// long, whichever comes first. The earlier attempt paired a batch target
    /// with the 1-second idle timeout, so any batch that never filled stalled
    /// for a second. Bounding the window makes the target safe: under load it
    /// fills instantly, and at low load it costs at most this much latency.
    batch_window_ns: i64 = 200_000,
    batch_waits: u64 = 0,
    spun: u64 = 0,
    tick_ts: linux.kernel_timespec = .{ .sec = 0, .nsec = 4_000_000 },
    timeout_arms: u64 = 0,

    /// SINGLE_ISSUER + DEFER_TASKRUN is the pair that makes completion mode
    /// batch. Without them the kernel runs task work on every interrupt and
    /// completions trickle out one at a time, which is exactly the collapse
    /// the first version measured. With them, task work is deferred until the
    /// ring is entered, so one `io_uring_enter` reaps a whole batch.
    ///
    /// SINGLE_ISSUER is a promise that only one thread ever submits. That is
    /// already true per carrier by construction, so it costs nothing.
    /// Registered ("fixed") files. Without this every SQE does an fget/fput
    /// on the fd table; with it the SQE carries a table index and IOSQE_FIXED_FILE
    /// and the kernel skips the lookup and the refcount churn. Registered as a
    /// sparse table of -1 and filled in per accept, so it costs one
    /// REGISTER_FILES_UPDATE per connection instead of a re-register.
    fixed_files: bool = true,
    file_slot: [sched.max_tasks]bool = @splat(false),
    file_updates: u64 = 0,
    feat: u32 = 0,
    /// Registered buffers cover the whole IoBuf store, so a send names an
    /// address inside registered memory. IORING_OP_WRITE_FIXED then skips the
    /// per-op page pinning that a plain send pays.
    fixed_bufs: bool = true,
    reg_iov: [1]std.posix.iovec = undefined,
    reg_base: usize = 0,
    reg_len: usize = 0,
    sends: u64 = 0,
    pool_slots: u16 = 0,
    /// Bumped whenever a task's registration is torn down, so any completion
    /// still in flight for the old incarnation is recognisably stale.
    arm_gen: [sched.max_tasks]u32 = @splat(0),
    stale_completions: u64 = 0,
    gen_keys: bool = true,
    /// Buffers the app is holding because it could not take their bytes.
    /// While these are held they are NOT in the ring, so the kernel runs short
    /// and stops delivering -- which is TCP backpressure expressed through the
    /// buffer ring. This is the flow control that readiness mode gets for free
    /// (a `read` takes only what fits) and completion mode otherwise lacks
    /// entirely, because the kernel has already read the bytes.
    held: u64 = 0,
    holds_total: u64 = 0,
    pauses: u64 = 0,
    /// Multishot recvs that ended and must be re-armed.
    ///
    /// A multishot recv TERMINATES on -ENOBUFS. If nothing re-arms it the
    /// connection is dead forever: it is not runnable, so no task ever runs to
    /// notice, and a client waiting on a response waits for good. Measured as
    /// a bimodal fairness result -- one client getting exactly 0 requests
    /// whenever the ring ran dry at the wrong moment (enobufs=52 in the
    /// failing runs, 20 in the healthy ones).
    ///
    /// Re-arming is deferred to the top of the next kernel step rather than
    /// done on the spot, because on the spot the ring is still empty and it
    /// would just terminate again in a tight loop. By the next step the
    /// completions in hand have been processed and their buffers returned.
    needs_rearm: [sched.max_tasks]bool = @splat(false),
    n_needs_rearm: usize = 0,
    rearms_after_end: u64 = 0,
    /// How many completions one `wait` will reap. Caps the batch size, so it
    /// is the single most direct throughput/latency knob on this reactor.
    cqe_batch: usize = max_cqes,

    fn prepRecvSqe(r: *Reactor, ud: u64, fd: i32) ?*linux.io_uring_sqe {
        const sqe = r.ring.get_sqe() catch return null;
        sqe.prep_rw(.RECV, fd, 0, 0, 0);
        sqe.rw_flags = 0;
        sqe.flags |= linux.IOSQE_BUFFER_SELECT;
        sqe.buf_index = buf_group;
        sqe.ioprio |= linux.IORING_RECV_MULTISHOT;
        sqe.user_data = ud;
        return sqe;
    }

    fn prepRecv(r: *Reactor, ud: u64, fd: i32, fixed: bool) !void {
        const sqe = r.prepRecvSqe(ud, fd) orelse return error.SubmissionQueueFull;
        if (fixed) sqe.flags |= linux.IOSQE_FIXED_FILE;
    }

    /// Bytes for a completion: a whole buffer, no shared head.
    fn bufFor(r: *Reactor, cqe: linux.io_uring_cqe) ?[]const u8 {
        if (cqe.flags & linux.IORING_CQE_F_BUFFER == 0) return null;
        const id: u16 = @intCast(cqe.flags >> linux.IORING_CQE_BUFFER_SHIFT);
        if (id >= r.n_bufs) return null;
        const off = @as(usize, r.buf_size) * id;
        return r.bufs_mem[off .. off + @as(usize, @intCast(cqe.res))];
    }

    /// Hand the buffer straight back to the ring.
    fn recycle(r: *Reactor, cqe: linux.io_uring_cqe) void {
        if (cqe.flags & linux.IORING_CQE_F_BUFFER == 0) return;
        r.recycleId(@intCast(cqe.flags >> linux.IORING_CQE_BUFFER_SHIFT));
    }

    fn recycleId(r: *Reactor, id: u16) void {
        if (id >= r.n_bufs) return;
        const off = @as(usize, r.buf_size) * id;
        IoUring.buf_ring_add(r.br, r.bufs_mem[off .. off + r.buf_size], id, r.br_mask, 0);
        IoUring.buf_ring_advance(r.br, 1);
    }

    pub fn registerFile(r: *Reactor, task: sched.TaskId, fd: i32) void {
        if (!r.fixed_files) return;
        const one = [_]i32{fd};
        r.ring.register_files_update(@intCast(task), &one) catch return;
        r.file_slot[task] = true;
        r.file_updates += 1;
    }

    pub fn unregisterFile(r: *Reactor, task: sched.TaskId) void {
        if (!r.fixed_files or !r.file_slot[task]) return;
        const none = [_]i32{-1};
        r.ring.register_files_update(@intCast(task), &none) catch {};
        r.file_slot[task] = false;
        r.file_updates += 1;
    }

    /// The fd (or table index) plus the flag an SQE needs.
    fn fileFor(r: *const Reactor, task: sched.TaskId, fd: i32) struct { fd: i32, fixed: bool } {
        if (r.fixed_files and r.file_slot[task]) return .{ .fd = @intCast(task), .fixed = true };
        return .{ .fd = fd, .fixed = false };
    }

    pub fn init(r: *Reactor, n_bufs: u16, buf_size: u32) !void {
        r.pool_slots = n_bufs;
        const flags: u32 = if (defer_taskrun)
            linux.IORING_SETUP_SINGLE_ISSUER | linux.IORING_SETUP_DEFER_TASKRUN
        else if (coop_taskrun)
            linux.IORING_SETUP_COOP_TASKRUN
        else
            0;
        r.ring = IoUring.init(queue_depth, flags) catch
            try IoUring.init(queue_depth, 0); // older kernel: fall back
        r.feat = r.ring.features;

        r.buf_size = buf_size;
        r.n_bufs = n_bufs;
        r.bufs_mem = try std.heap.page_allocator.alloc(u8, @as(usize, buf_size) * n_bufs);
        // Not IoUring.setup_buf_ring: see uring_bufring.zig. It registers the
        // correct way first and only works around a broken kernel if that is
        // refused with EINVAL.
        r.br = try @import("uring_bufring.zig").setup(r.ring.fd, n_bufs, buf_group);
        IoUring.buf_ring_init(r.br);
        r.br_mask = IoUring.buf_ring_mask(n_bufs);
        var bi: u16 = 0;
        while (bi < n_bufs) : (bi += 1) {
            const off = @as(usize, buf_size) * bi;
            IoUring.buf_ring_add(r.br, r.bufs_mem[off .. off + buf_size], bi, r.br_mask, bi);
        }
        IoUring.buf_ring_advance(r.br, n_bufs);

        if (r.fixed_bufs) {
            const region = try @import("iobuf.zig").storeRegion(r.pool_slots);
            r.reg_base = @intFromPtr(region.ptr);
            r.reg_len = region.len;
            r.reg_iov[0] = .{ .base = region.ptr, .len = region.len };
            r.ring.register_buffers(&r.reg_iov) catch {
                r.fixed_bufs = false;
            };
        }

        // Sparse file table: every slot -1, filled per accept.
        if (r.fixed_files) {
            const sparse = try std.heap.page_allocator.alloc(i32, sched.max_tasks);
            defer std.heap.page_allocator.free(sparse);
            @memset(sparse, -1);
            r.ring.register_files(sparse) catch {
                r.fixed_files = false;
            };
        }
    }

    /// One submission for the connection's entire life.
    pub fn armRecv(r: *Reactor, task: sched.TaskId, fd: i32) void {
        const f = r.fileFor(task, fd);
        if (f.fixed) {
            if (r.prepRecvSqe(key(tag_recv, task, r.arm_gen[task]), f.fd)) |sqe| {
                sqe.flags |= linux.IOSQE_FIXED_FILE;
                r.sqes_total += 1;
                r.recv_arms += 1;
                if (!r.armed_recv[task]) {
                    r.armed_recv[task] = true;
                    r.armed += 1;
                }
                return;
            }
        }
        r.prepRecv(key(tag_recv, task, r.arm_gen[task]), fd, false) catch {
            _ = r.ring.submit() catch return;
            r.enters += 1;
            r.prepRecv(key(tag_recv, task, r.arm_gen[task]), fd, false) catch return;
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
        _ = r.ring.poll_add(key(tag_poll, task, r.arm_gen[task]), fd, mask) catch return;
        r.sqes_total += 1;
        r.armed_poll[task] = true;
    }

    /// Stop receiving on this connection. The multishot recv is cancelled, so
    /// the kernel stops reading the socket, the TCP window closes and the peer
    /// stalls. This is the ONLY per-connection flow-control lever in
    /// completion mode -- withholding a buffer throttles the whole pool
    /// instead, because multishot simply uses a different buffer.
    pub fn pauseRecv(r: *Reactor, task: sched.TaskId) void {
        if (!r.armed_recv[task]) return;
        _ = r.ring.poll_remove(tag_timeout, key(tag_recv, task, r.arm_gen[task])) catch {};
        r.armed_recv[task] = false;
        if (r.armed > 0) r.armed -= 1;
        r.arm_gen[task] +%= 1; // any completion still in flight is now stale
        r.pauses += 1;
    }

    pub fn unwatch(r: *Reactor, task: sched.TaskId) void {
        if (r.armed_recv[task]) {
            _ = r.ring.poll_remove(tag_timeout, key(tag_recv, task, r.arm_gen[task])) catch {};
            r.armed_recv[task] = false;
            if (r.armed > 0) r.armed -= 1;
        }
        r.armed_poll[task] = false;
        // Invalidate every key still outstanding for this incarnation.
        if (use_gen) r.arm_gen[task] +%= 1;
    }

    /// Submit a send. The buffer must stay valid until completion, which the
    /// pool slot guarantees: it is held by the connection for the whole write.
    pub fn submitSend(r: *Reactor, task: sched.TaskId, fd: i32, bytes: []const u8) bool {
        const f = r.fileFor(task, fd);
        const ud = key(tag_send, task, r.arm_gen[task]);
        const addr = @intFromPtr(bytes.ptr);
        const in_region = r.fixed_bufs and addr >= r.reg_base and addr + bytes.len <= r.reg_base + r.reg_len;

        const sqe = blk: {
            if (in_region) {
                var iov: std.posix.iovec = .{ .base = @constCast(bytes.ptr), .len = bytes.len };
                break :blk r.ring.write_fixed(ud, f.fd, &iov, 0, 0) catch null;
            }
            break :blk r.ring.send(ud, f.fd, bytes, 0) catch null;
        } orelse return false;

        if (f.fixed) sqe.flags |= linux.IOSQE_FIXED_FILE;
        r.sqes_total += 1;
        r.sends += 1;
        return true;
    }

    pub fn watching(r: *const Reactor, task: sched.TaskId) bool {
        return r.armed_recv[task];
    }

    /// Drain the set of connections whose multishot recv ended. The caller
    /// re-arms the ones still alive.
    pub fn takeRearms(r: *Reactor, out: []sched.TaskId) usize {
        if (r.n_needs_rearm == 0) return 0;
        var n: usize = 0;
        for (r.needs_rearm, 0..) |need, i| {
            if (!need) continue;
            r.needs_rearm[i] = false;
            r.n_needs_rearm -= 1;
            if (n < out.len) {
                out[n] = @intCast(i);
                n += 1;
            }
            if (r.n_needs_rearm == 0) break;
        }
        r.rearms_after_end += n;
        return n;
    }

    /// Drain one completion. The returned `data` slice points into the kernel
    /// buffer ring and is valid until `release` is called for it.
    pub fn next(r: *Reactor) ?Completion {
        if (r.done_i >= r.done_n) return null;
        const c = r.done[r.done_i];
        r.done_i += 1;
        return c;
    }

    /// The app consumed the bytes: return the buffer to the ring.
    pub fn release(r: *Reactor, c: Completion) void {
        if (!c.has_buf) return;
        r.recycleId(c.buf_id);
        if (r.held > 0) r.held -= 1;
    }

    /// The app could not take the bytes. Keep the buffer OUT of the ring.
    /// Enough of these and the kernel returns -ENOBUFS and stops delivering,
    /// which is exactly the flow control we want -- the peer's window closes
    /// instead of us refusing a request that was never malformed.
    pub fn hold(r: *Reactor, c: Completion) void {
        if (!c.has_buf) return;
        r.held += 1;
        r.holds_total += 1;
    }

    /// Return a buffer the app was holding, once it has room again.
    pub fn releaseHeld(r: *Reactor, buf_id: u16) void {
        r.recycleId(buf_id);
        if (r.held > 0) r.held -= 1;
    }

    pub fn wait(r: *Reactor, s: *sched.Sched, timeout_ms: i32) usize {
        r.waits += 1;
        r.fds_polled += r.armed;
        r.done_n = 0;
        r.done_i = 0;
        r.stage_n = 0;

        // `submit_and_wait(N)` blocks until N CQEs exist and a timeout only
        // produces one, so pairing it with a batch target waits for real
        // completions that may never come. Wake on the FIRST, then reap
        // non-blocking for a bounded window to let stragglers accumulate.
        if (timeout_ms > 0 and !r.tick_armed) {
            r.tick_ts = .{ .sec = 0, .nsec = r.tick_ns };
            _ = r.ring.timeout(tag_timeout, &r.tick_ts, 0, 0) catch {};
            r.sqes_total += 1;
            r.timeout_arms += 1;
            r.tick_armed = true;
        }
        const wait_nr: u32 = if (timeout_ms == 0) 0 else 1;
        _ = r.ring.submit_and_wait(wait_nr) catch {
            _ = r.ring.submit() catch {};
            r.enters += 1;
            return 0;
        };
        r.enters += 1;

        var cqes: [max_cqes]linux.io_uring_cqe = undefined;
        const cap = @min(r.cqe_batch, max_cqes);
        var n = r.ring.copy_cqes(cqes[0..cap], 0) catch return 0;
        // Bounded non-blocking spin: only entered when we already have work,
        // so it never adds idle latency. It trades CPU for batch size.
        if (r.batch_target > 1 and n > 0 and n < r.batch_target) {
            const deadline = nowNsLocal() + r.batch_window_ns;
            while (n < r.batch_target and nowNsLocal() < deadline) {
                const extra = r.ring.copy_cqes(cqes[n..], 0) catch break;
                if (extra == 0) {
                    std.atomic.spinLoopHint();
                    continue;
                }
                n += extra;
                r.spun += extra;
            }
        }
        r.cqes_total += n;
        for (cqes[0..n]) |cqe| {
            if (cqe.user_data == tag_timeout) {
                r.tick_armed = false;
                continue;
            }
            const task = keyTask(cqe.user_data);
            if (use_gen and keyGen(cqe.user_data) != r.arm_gen[task]) {
                // Late completion for a released incarnation. Recycle its
                // buffer and drop it -- delivering it would hand one
                // connection's bytes to whoever inherited the slot.
                r.stale_completions += 1;
                r.recycle(cqe);
                continue;
            }

            if (cqe.user_data & tag_send != 0) {
                r.push(.{ .task = task, .kind = .sent, .sent = cqe.res });
                s.makeRunnable(task, .io);
                continue;
            }
            if (cqe.user_data & tag_poll != 0) {
                // Readiness, as before: the task wakes and does its own I/O.
                r.armed_poll[task] = false;
                s.makeRunnable(task, .io);
                continue;
            }

            // recv completion
            const more = cqe.flags & linux.IORING_CQE_F_MORE != 0;
            if (!more and r.armed_recv[task]) {
                if (!r.needs_rearm[task]) {
                    r.needs_rearm[task] = true;
                    r.n_needs_rearm += 1;
                }
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
            const data = r.bufFor(cqe) orelse {
                r.push(.{ .task = task, .kind = .err });
                s.makeRunnable(task, .io);
                continue;
            };
            // Copy out of the ring buffer, but do NOT return it yet. The app
            // decides: `release` puts it back, `hold` keeps it out of the ring
            // and applies backpressure.
            const room = r.stage.len - r.stage_n;
            const take = @min(room, data.len);
            @memcpy(r.stage[r.stage_n..][0..take], data[0..take]);
            const slice = r.stage[r.stage_n..][0..take];
            r.stage_n += take;
            r.bytes_in += take;
            const bid: u16 = @intCast(cqe.flags >> linux.IORING_CQE_BUFFER_SHIFT);
            r.push(.{ .task = task, .kind = .data, .data = slice, .has_buf = true, .buf_id = bid });
        }
        return r.done_n;
    }

    fn push(r: *Reactor, c: Completion) void {
        if (r.done_n == r.done.len) return;
        r.done[r.done_n] = c;
        r.done_n += 1;
    }
};
