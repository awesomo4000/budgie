//! The epoll half of the readiness reactor.
//!
//! Everything that knows the word "epoll" is in this file. `reactor.zig` owns
//! the client table, the byte accounting and the round policy, and reaches the
//! kernel only through the four functions below.
//!
//! EPOLLONESHOT is what makes the semantics line up exactly. The poll version
//! removed a task from the poll set the moment it became ready, so the set
//! always held only the parked population. ONESHOT gets that from the kernel:
//! after an event fires the fd is disarmed until explicitly rearmed with
//! EPOLL_CTL_MOD. So `wait` needs no bookkeeping at all on the ready path, and
//! parking costs exactly one `epoll_ctl`.

const std = @import("std");
const linux = std.os.linux;
const Interest = @import("interest.zig").Interest;

pub const TaskId = @import("sched.zig").TaskId;

fn sysErr(rc: usize) bool {
    return @as(isize, @bitCast(rc)) < 0;
}

fn events(i: Interest) u32 {
    const base: u32 = switch (i) {
        .read => linux.EPOLL.IN,
        .write => linux.EPOLL.OUT,
    };
    return base | linux.EPOLL.ONESHOT | linux.EPOLL.RDHUP;
}

pub const Backend = struct {
    epfd: i32 = -1,

    pub fn init(be: *Backend) !void {
        const rc = linux.epoll_create1(0);
        if (sysErr(rc)) return error.EpollCreateFailed;
        be.epfd = @intCast(rc);
    }

    pub fn deinit(be: *Backend) void {
        if (be.epfd >= 0) _ = linux.close(be.epfd);
        be.epfd = -1;
    }

    /// Arm oneshot interest. `rearm` is true if this fd has been armed before,
    /// which is exactly the ADD/MOD distinction ONESHOT expects. Returns false
    /// if the kernel refused, in which case nothing was armed.
    pub fn arm(be: *Backend, task: TaskId, fd: i32, i: Interest, rearm: bool) bool {
        var ev = linux.epoll_event{
            .events = events(i),
            .data = .{ .u64 = task },
        };
        const op: u32 = if (rearm) linux.EPOLL.CTL_MOD else linux.EPOLL.CTL_ADD;
        return !sysErr(linux.epoll_ctl(be.epfd, op, fd, &ev));
    }

    pub fn disarm(be: *Backend, task: TaskId, fd: i32) void {
        _ = task;
        _ = linux.epoll_ctl(be.epfd, linux.EPOLL.CTL_DEL, fd, null);
    }

    /// Block up to `timeout_ms` and fill `out` with the ready tasks. Returns
    /// how many were written. The kernel hands back only the ready set, so
    /// this is O(ready) rather than O(registered).
    pub fn wait(be: *Backend, out: []TaskId, timeout_ms: i32) usize {
        var evs: [256]linux.epoll_event = undefined;
        const cap = @min(out.len, evs.len);
        const rc = linux.epoll_wait(be.epfd, &evs, @intCast(cap), timeout_ms);
        if (sysErr(rc)) return 0;
        const n: usize = rc;
        for (evs[0..n], 0..) |ev, k| out[k] = @intCast(ev.data.u64);
        return n;
    }

    /// The reactor does the read itself so that it sees the bytes; see the
    /// comment on `Reactor.read`. Negative means -errno, as the raw syscall
    /// returns it.
    pub fn read(fd: i32, buf: []u8) isize {
        return @bitCast(linux.read(fd, buf.ptr, buf.len));
    }
};
