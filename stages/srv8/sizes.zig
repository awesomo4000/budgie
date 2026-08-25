const std = @import("std");
const S = @import("sched.zig");
const R = @import("reactor.zig");
const http = @import("http.zig");
pub fn main() !void {
    std.debug.print("{s:<28} {s:>12} {s:>10}\n", .{ "struct", "bytes", "MB" });
    inline for (.{
        .{ "sched.Sched", @sizeOf(S.Sched) },
        .{ "reactor.Reactor", @sizeOf(R.Reactor) },
        .{ "http.Parser", @sizeOf(http.Parser) },
    }) |e| std.debug.print("{s:<28} {d:>12} {d:>10.2}\n", .{ e[0], e[1], @as(f64, @floatFromInt(e[1])) / 1048576.0 });
    std.debug.print("\nmax_tasks = {d}, prio_levels = {d}, wheel_slots = {d}\n", .{ S.max_tasks, S.prio_levels, S.wheel_slots });
}
