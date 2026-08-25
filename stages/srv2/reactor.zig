//! The reactor. Maps file descriptors to task ids and reports readiness.
//!
//! It does not know about budgets, cleanup reserves, phases, or deadlines. It
//! takes a timeout it was handed and it marks tasks runnable. Swapping `poll`
//! for `epoll` changes this file and nothing else.
//!
//! Registration is O(1): a dense pollfd array plus a task -> index map, with
//! swap-remove on unregister.

const std = @import("std");
const posix = std.posix;
const sched = @import("sched.zig");

pub const Interest = enum {
    read,
    write,

    fn events(i: Interest) i16 {
        return switch (i) {
            .read => posix.POLL.IN,
            .write => posix.POLL.OUT,
        };
    }
};

pub const Reactor = struct {
    fds: [sched.max_tasks]posix.pollfd = undefined,
    owner: [sched.max_tasks]sched.TaskId = undefined,
    n: usize = 0,
    slot: [sched.max_tasks]u32 = @splat(no_slot),

    waits: u64 = 0,
    fds_polled: u64 = 0,

    const no_slot: u32 = std.math.maxInt(u32);

    pub fn watch(r: *Reactor, task: sched.TaskId, fd: i32, i: Interest) void {
        if (r.slot[task] != no_slot) {
            r.fds[r.slot[task]].events = i.events();
            return;
        }
        const idx: u32 = @intCast(r.n);
        r.fds[idx] = .{ .fd = fd, .events = i.events(), .revents = 0 };
        r.owner[idx] = task;
        r.slot[task] = idx;
        r.n += 1;
    }

    pub fn unwatch(r: *Reactor, task: sched.TaskId) void {
        const idx = r.slot[task];
        if (idx == no_slot) return;
        r.slot[task] = no_slot;
        r.n -= 1;
        if (idx != r.n) {
            r.fds[idx] = r.fds[r.n];
            r.owner[idx] = r.owner[r.n];
            r.slot[r.owner[idx]] = idx;
        }
    }

    pub fn watching(r: *const Reactor, task: sched.TaskId) bool {
        return r.slot[task] != no_slot;
    }

    /// Block up to `timeout_ms` and mark every ready task runnable.
    /// Returns how many became ready.
    pub fn wait(r: *Reactor, s: *sched.Sched, timeout_ms: i32) usize {
        r.waits += 1;
        r.fds_polled += r.n;
        const ready = posix.poll(r.fds[0..r.n], timeout_ms) catch return 0;
        if (ready == 0) return 0;

        var seen: usize = 0;
        var i: usize = 0;
        while (i < r.n and seen < ready) : (i += 1) {
            if (r.fds[i].revents == 0) continue;
            seen += 1;
            const task = r.owner[i];
            // A ready task stops being watched; the app re-arms when it parks
            // again. Keeps the poll set to exactly the parked population.
            r.unwatch(task);
            s.makeRunnable(task, .io);
            i -= 1; // swap-remove moved a new entry into this index
        }
        return seen;
    }
};
