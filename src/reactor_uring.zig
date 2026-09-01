//! The reactor, on io_uring. Same public surface as the epoll and poll
//! versions:
//!
//!     watch(task, fd, interest)   unwatch(task)   watching(task)
//!     wait(sched, timeout_ms) -> marks ready tasks runnable
//!
//! This variant uses IORING_OP_POLL_ADD, which keeps io_uring in *readiness*
//! mode: submit "tell me when this fd is readable", get a completion saying it
//! is, then the application does the read itself. That is deliberately the
//! conservative port -- it isolates the syscall-batching win from the much
//! larger interface change that true completion-based I/O demands.
//!
//! What it buys over epoll:
//!   - arming N fds costs ONE `io_uring_enter`, not N `epoll_ctl` calls.
//!     Submissions accumulate in a shared ring and go out with the next wait.
//!   - the wait and the submit are the same syscall (`submit_and_wait`).
//!
//! What it does not buy: the read and write are still ordinary syscalls. That
//! is what `reactor_uring_rw.zig` changes, and why it cannot keep this
//! interface.
//!
//! POLL_ADD is one-shot by default, which lines up exactly with how the epoll
//! build used EPOLLONESHOT: a ready task leaves the interest set and re-arms
//! when it parks again.

const std = @import("std");
const linux = std.os.linux;
const IoUring = linux.IoUring;
const sched = @import("sched.zig");

/// The same type `reactor.zig` uses, not a copy of it. They were structurally
/// identical and separately declared, which is exactly how two backends drift
/// apart without anyone noticing: the application names one of them, and
/// swapping reactors then fails to compile for a reason that has nothing to do
/// with the reactor.
pub const Interest = @import("interest.zig").Interest;
pub const Violation = @import("invariant.zig").Violation;

/// No byte fairness here. See `reactor.zig` for what the flag is for; the
/// honest answer for this backend is that `read` is a plain syscall with
/// nothing metering it, and saying so is better than growing inert `quantum`
/// and `auto` fields that take a value and do nothing with it.
pub const has_byte_fairness = false;

fn mask(i: Interest) u32 {
    return switch (i) {
        .read => linux.POLL.IN,
        .write => linux.POLL.OUT,
    };
}

const queue_depth: u16 = 4096;
const max_cqes = 1024;

pub const Reactor = struct {
    ring: IoUring = undefined,
    armed_now: [sched.max_tasks]bool = @splat(false),
    armed: usize = 0,
    /// Submissions queued but not yet handed to the kernel. The whole point:
    /// this counter is what an epoll build would have paid as syscalls.
    queued_sqes: u64 = 0,

    waits: u64 = 0,
    fds_polled: u64 = 0,
    enters: u64 = 0,
    sqes_total: u64 = 0,
    overflow_submits: u64 = 0,
    bytes_in: u64 = 0,

    pub fn init(r: *Reactor) !void {
        r.ring = try IoUring.init(queue_depth, 0);
    }

    pub fn deinit(r: *Reactor) void {
        r.ring.deinit();
    }

    // ------------------------------------------------- the rest of the shape
    //
    // `app/server.zig` grew a wider reactor interface than this file had, and
    // nobody noticed because nothing built the server against it. The claim in
    // the README that swapping backends touches `reactor.zig` only had quietly
    // stopped being true for this one.
    //
    // These are the difference, and they are honest rather than decorative:
    // readiness mode does its own reads, and this backend has no byte-fairness
    // to account for. Saying "no throttling" is a true statement about this
    // reactor, not a stub that pretends.

    /// Readiness mode does the read itself, so this is the plain syscall. The
    /// epoll reactor also meters bytes here for DRR; there is nothing to meter
    /// when there is no DRR.
    pub fn read(r: *Reactor, task: sched.TaskId, fd: i32, buf: []u8) isize {
        _ = task;
        const n = linux.read(fd, buf.ptr, buf.len);
        const sn: isize = @bitCast(n);
        if (sn > 0) r.bytes_in += @intCast(sn);
        return sn;
    }

    /// No per-client state, so nothing to set up or tear down.
    pub fn open(r: *Reactor, task: sched.TaskId) void {
        _ = r;
        _ = task;
    }

    pub fn close(r: *Reactor, task: sched.TaskId) void {
        _ = r;
        _ = task;
    }

    /// No byte-fairness on this backend. A round never completes because there
    /// are no rounds, and nobody is ever throttled.
    pub fn advanceRound(r: *Reactor) usize {
        _ = r;
        return 0;
    }

    pub fn throttledCount(r: *const Reactor) usize {
        _ = r;
        return 0;
    }

    pub fn roundComplete(r: *const Reactor) bool {
        _ = r;
        return false;
    }

    pub fn watch(r: *Reactor, task: sched.TaskId, fd: i32, i: Interest) void {
        _ = r.ring.poll_add(@as(u64, task), fd, mask(i)) catch {
            // Submission ring full: flush and retry once. Rare, and counted so
            // it cannot hide.
            r.overflow_submits += 1;
            _ = r.ring.submit() catch return;
            r.enters += 1;
            r.queued_sqes = 0;
            _ = r.ring.poll_add(@as(u64, task), fd, mask(i)) catch return;
        };
        r.queued_sqes += 1;
        r.sqes_total += 1;
        if (!r.armed_now[task]) {
            r.armed_now[task] = true;
            r.armed += 1;
        }
    }

    pub fn unwatch(r: *Reactor, task: sched.TaskId) void {
        if (!r.armed_now[task]) return;
        // Cancel the outstanding poll. user_data on the cancel is tagged so
        // its completion is not mistaken for a readiness event.
        _ = r.ring.poll_remove(cancel_tag | @as(u64, task), @as(u64, task)) catch {};
        r.queued_sqes += 1;
        r.armed_now[task] = false;
        if (r.armed > 0) r.armed -= 1;
    }

    /// Nothing to check here yet.
    ///
    /// The readiness reactor over io_uring keeps a submission queue and a set
    /// of armed tasks, and both are already covered by what the scheduler and
    /// the application assert about parked tasks. The IOCP backend has a
    /// conservation law of its own because the kernel holds blocks it is not
    /// finished with; nothing in this file does. Present so the application can
    /// ask every reactor the same question.
    pub fn check(r: *const Reactor) ?Violation {
        _ = r;
        return null;
    }

    pub fn watching(r: *const Reactor, task: sched.TaskId) bool {
        return r.armed_now[task];
    }

    const cancel_tag: u64 = 1 << 40;

    /// One `io_uring_enter` submits every queued arm AND waits. An epoll build
    /// would have spent one `epoll_ctl` per arm plus one `epoll_wait`.
    pub fn wait(r: *Reactor, s: *sched.Sched, timeout_ms: i32) usize {
        r.waits += 1;
        r.fds_polled += r.armed;

        var ts: linux.kernel_timespec = .{
            .sec = @divTrunc(timeout_ms, 1000),
            .nsec = @as(i64, @intCast(@mod(timeout_ms, 1000))) * 1_000_000,
        };
        if (timeout_ms > 0) {
            _ = r.ring.timeout(timeout_tag, &ts, 0, 0) catch {};
            r.queued_sqes += 1;
        }

        const wait_nr: u32 = if (timeout_ms == 0) 0 else 1;
        _ = r.ring.submit_and_wait(wait_nr) catch return 0;
        r.enters += 1;
        r.queued_sqes = 0;

        var cqes: [max_cqes]linux.io_uring_cqe = undefined;
        const n = r.ring.copy_cqes(&cqes, 0) catch return 0;
        var ready: usize = 0;
        for (cqes[0..n]) |cqe| {
            if (cqe.user_data == timeout_tag) continue;
            if (cqe.user_data & cancel_tag != 0) continue; // poll_remove ack
            const task: sched.TaskId = @intCast(cqe.user_data);
            if (cqe.res < 0) continue; // cancelled poll
            if (r.armed_now[task]) {
                r.armed_now[task] = false;
                if (r.armed > 0) r.armed -= 1;
            }
            s.makeRunnable(task, .io);
            ready += 1;
        }
        return ready;
    }

    const timeout_tag: u64 = 1 << 41;
};
