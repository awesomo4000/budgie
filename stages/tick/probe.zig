const std = @import("std");
const linux = std.os.linux;
const posix = std.posix;

fn onSig(_: posix.SIG) callconv(.c) void {
    std.debug.print("sig!\n", .{});
}

const timeval = extern struct { sec: isize, usec: isize };
const itimerval = extern struct { it_interval: timeval, it_value: timeval };
const ITIMER_REAL: usize = 0;

pub fn main() !void {
    var act: posix.Sigaction = .{
        .handler = .{ .handler = onSig },
        .mask = posix.sigemptyset(),
        .flags = posix.SA.RESTART,
    };
    posix.sigaction(.ALRM, &act, null);

    const v: itimerval = .{
        .it_interval = .{ .sec = 0, .usec = 200_000 },
        .it_value = .{ .sec = 0, .usec = 200_000 },
    };
    const rc = linux.syscall3(.setitimer, ITIMER_REAL, @intFromPtr(&v), 0);
    std.debug.print("setitimer rc={d}\n", .{rc});

    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    std.debug.print("mono {d}.{d}\n", .{ ts.sec, ts.nsec });

    var i: u64 = 0;
    while (i < 400_000_000) : (i += 1) std.mem.doNotOptimizeAway(i);
    std.debug.print("done\n", .{});
}
