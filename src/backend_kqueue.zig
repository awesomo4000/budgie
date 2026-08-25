//! The kqueue half of the readiness reactor, for macOS and the BSDs.
//!
//! Same four functions as the epoll backend, and the semantics line up almost
//! exactly, because kqueue has `EV_ONESHOT` natively. After an event fires the
//! knote is removed and the task stays parked until rearmed, which is the
//! property the whole reactor is built on.
//!
//! Three differences from epoll, all absorbed here:
//!
//!   - `EV_ADD` is an upsert, so there is no ADD/MOD distinction. The `rearm`
//!     flag the epoll backend needs is accepted and ignored.
//!   - Interest is a *filter*, not a bit in a mask: a read and a write on one
//!     fd are two separate knotes. Since a task waits on one direction at a
//!     time, switching direction has to delete the old filter or it lingers
//!     and fires later against a task that is no longer waiting for it. That
//!     is what `filter_of` is for.
//!   - There is no RDHUP to ask for. A peer hangup arrives as `EV_EOF` on the
//!     read filter, which is already a readable event, so the reactor's read
//!     returns 0 and the existing peer_gone path handles it unchanged.

const std = @import("std");
const c = std.c;
const Interest = @import("interest.zig").Interest;

const sched = @import("sched.zig");

pub const TaskId = sched.TaskId;

/// Tasks index `filter_of` directly, so this tracks the scheduler's task space
/// rather than restating it -- a second copy of the bound is a silent
/// out-of-bounds waiting for someone to raise one of the two.
const max_tasks = sched.max_tasks;

const filter_none: i16 = 0;

fn filterFor(i: Interest) i16 {
    return switch (i) {
        .read => c.EVFILT.READ,
        .write => c.EVFILT.WRITE,
    };
}

pub const Backend = struct {
    kq: i32 = -1,
    /// Which filter each task currently has registered, so that a direction
    /// switch or an unwatch can delete the right one. `filter_none` means
    /// nothing is registered.
    filter_of: [max_tasks]i16 = @splat(filter_none),

    pub fn init(be: *Backend) !void {
        const rc = c.kqueue();
        if (rc < 0) return error.KqueueCreateFailed;
        be.kq = rc;
    }

    pub fn deinit(be: *Backend) void {
        if (be.kq >= 0) _ = c.close(be.kq);
        be.kq = -1;
    }

    pub fn arm(be: *Backend, task: TaskId, fd: i32, i: Interest, rearm: bool) bool {
        _ = rearm; // EV_ADD is an upsert; the distinction is epoll's, not ours.
        const want = filterFor(i);

        // Switching direction: drop the old knote first. Without this a stale
        // read filter stays armed while the task waits to write, and fires
        // into a task that is not parked on it.
        const had = be.filter_of[task];
        if (had != filter_none and had != want) be.del(fd, had);

        var kev = c.Kevent{
            .ident = @intCast(fd),
            .filter = want,
            .flags = c.EV.ADD | c.EV.ENABLE | c.EV.ONESHOT,
            .fflags = 0,
            .data = 0,
            .udata = @intCast(task),
        };
        if (c.kevent(be.kq, @ptrCast(&kev), 1, undefined, 0, null) < 0) {
            be.filter_of[task] = filter_none;
            return false;
        }
        be.filter_of[task] = want;
        return true;
    }

    pub fn disarm(be: *Backend, task: TaskId, fd: i32) void {
        const had = be.filter_of[task];
        if (had == filter_none) return;
        be.del(fd, had);
        be.filter_of[task] = filter_none;
    }

    fn del(be: *Backend, fd: i32, filter: i16) void {
        var kev = c.Kevent{
            .ident = @intCast(fd),
            .filter = filter,
            .flags = c.EV.DELETE,
            .fflags = 0,
            .data = 0,
            .udata = 0,
        };
        // ENOENT is expected and ignored: EV_ONESHOT already removed the knote
        // when it fired, so most deletes on a woken task find nothing. epoll
        // differs here -- ONESHOT leaves the fd registered but disabled, so its
        // CTL_DEL succeeds -- but both paths ignore the result, so the reactor
        // above cannot tell the difference.
        _ = c.kevent(be.kq, @ptrCast(&kev), 1, undefined, 0, null);
    }

    pub fn wait(be: *Backend, out: []TaskId, timeout_ms: i32) usize {
        var evs: [256]c.Kevent = undefined;
        const cap = @min(out.len, evs.len);

        var ts: c.timespec = undefined;
        const tp: ?*const c.timespec = if (timeout_ms < 0) null else blk: {
            ts = .{
                .sec = @divTrunc(timeout_ms, 1000),
                .nsec = @as(isize, @intCast(@mod(timeout_ms, 1000))) * 1_000_000,
            };
            break :blk &ts;
        };

        const rc = c.kevent(be.kq, undefined, 0, &evs, @intCast(cap), tp);
        if (rc < 0) return 0;
        const n: usize = @intCast(rc);
        var k: usize = 0;
        for (evs[0..n]) |ev| {
            // EV_ERROR reports a bad change request, not a ready fd. Waking the
            // task on it would hand the reactor a readiness it never got.
            if (ev.flags & c.EV.ERROR != 0) continue;
            const task: TaskId = @intCast(ev.udata);
            if (task < max_tasks) be.filter_of[task] = filter_none; // ONESHOT fired
            out[k] = task;
            k += 1;
        }
        return k;
    }

    pub fn read(fd: i32, buf: []u8) isize {
        // Darwin's read already returns -1 on error rather than -errno. The
        // reactor only ever tests the sign, so the two backends agree on
        // everything it actually looks at.
        return c.read(fd, buf.ptr, buf.len);
    }
};
