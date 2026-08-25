//! The reactor, on epoll. Same public surface as the poll version:
//!
//!     watch(task, fd, interest)   unwatch(task)   watching(task)
//!     wait(sched, timeout_ms) -> marks ready tasks runnable
//!
//! Nothing outside this file changed. sched.zig and server.zig are untouched.
//!
//! EPOLLONESHOT is what makes the semantics line up exactly. The poll version
//! removed a task from the poll set the moment it became ready, so the set
//! always held only the parked population. ONESHOT gets that from the kernel:
//! after an event fires, the fd is disarmed until explicitly rearmed with
//! EPOLL_CTL_MOD. So `wait` needs no bookkeeping at all on the ready path,
//! and parking costs exactly one `epoll_ctl`.

const std = @import("std");
const linux = std.os.linux;
const sched = @import("sched.zig");

pub const Interest = enum {
    read,
    write,

    fn events(i: Interest) u32 {
        const base: u32 = switch (i) {
            .read => linux.EPOLL.IN,
            .write => linux.EPOLL.OUT,
        };
        return base | linux.EPOLL.ONESHOT | linux.EPOLL.RDHUP;
    }
};

const max_events = 1024;

fn sysErr(rc: usize) bool {
    return @as(isize, @bitCast(rc)) < 0;
}

pub const Reactor = struct {
    epfd: i32 = -1,
    /// Whether the fd for this task has ever been added. After the first ADD
    /// every rearm is a MOD, which is what ONESHOT expects.
    added: [sched.max_tasks]bool = @splat(false),
    fd_of: [sched.max_tasks]i32 = @splat(-1),
    armed: usize = 0,

    waits: u64 = 0,
    fds_polled: u64 = 0, // kept for stat parity: counts armed fds at wait time
    ctls: u64 = 0,

    pub fn init(r: *Reactor) !void {
        const rc = linux.epoll_create1(0);
        if (sysErr(rc)) return error.EpollCreateFailed;
        r.epfd = @intCast(rc);
    }

    pub fn watch(r: *Reactor, task: sched.TaskId, fd: i32, i: Interest) void {
        var ev = linux.epoll_event{
            .events = i.events(),
            .data = .{ .u64 = task },
        };
        const op: u32 = if (r.added[task]) linux.EPOLL.CTL_MOD else linux.EPOLL.CTL_ADD;
        r.ctls += 1;
        if (sysErr(linux.epoll_ctl(r.epfd, op, fd, &ev))) return;
        if (!r.added[task]) {
            r.added[task] = true;
            r.fd_of[task] = fd;
        }
        r.armed += 1;
    }

    pub fn unwatch(r: *Reactor, task: sched.TaskId) void {
        if (!r.added[task]) return;
        r.ctls += 1;
        _ = linux.epoll_ctl(r.epfd, linux.EPOLL.CTL_DEL, r.fd_of[task], null);
        r.added[task] = false;
        r.fd_of[task] = -1;
        if (r.armed > 0) r.armed -= 1;
    }

    pub fn watching(r: *const Reactor, task: sched.TaskId) bool {
        return r.added[task];
    }

    /// Block up to `timeout_ms` and mark every ready task runnable.
    ///
    /// The kernel hands back only the ready set, so this is O(ready) rather
    /// than O(registered). That is the entire difference from the poll build.
    pub fn wait(r: *Reactor, s: *sched.Sched, timeout_ms: i32) usize {
        var evs: [max_events]linux.epoll_event = undefined;
        r.waits += 1;
        r.fds_polled += r.armed;
        const rc = linux.epoll_wait(r.epfd, &evs, max_events, timeout_ms);
        if (sysErr(rc)) return 0;
        const n: usize = rc;
        for (evs[0..n]) |ev| {
            const task: sched.TaskId = @intCast(ev.data.u64);
            // ONESHOT already disarmed it kernel-side; mirror that locally so
            // the next park issues a MOD rather than a redundant ADD.
            if (r.armed > 0) r.armed -= 1;
            s.makeRunnable(task, .io);
        }
        return n;
    }
};
