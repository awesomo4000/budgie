const std = @import("std");
const linux = std.os.linux;
const posix = std.posix;

var hits: u32 = 0;
var sink: u64 = 0;
fn onSig(_: posix.SIG) callconv(.c) void { hits +%= 1; }

const timeval = extern struct { sec: isize, usec: isize };
const itimerval = extern struct { it_interval: timeval, it_value: timeval };

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
    std.debug.print("setitimer rc={d}\n", .{linux.syscall3(.setitimer, 0, @intFromPtr(&v), 0)});

    var t0: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &t0);
    var i: u64 = 0;
    while (i < 300_000_000) : (i += 1) {
        @atomicStore(u64, &sink, @atomicLoad(u64, &sink, .monotonic) +% i, .monotonic);
    }
    var t1: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &t1);
    const ms = (t1.sec - t0.sec) * 1000 + @divTrunc(t1.nsec - t0.nsec, 1_000_000);
    std.debug.print("elapsed={d}ms hits={d}\n", .{ ms, @atomicLoad(u32, &hits, .monotonic) });
}
