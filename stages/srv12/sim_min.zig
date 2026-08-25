const std = @import("std");
const sched = @import("sched.zig");
pub fn main() !void {
    std.debug.print("sizeof(Sched) = {d} bytes ({d:.2} MB)\n", .{ @sizeOf(sched.Sched), @as(f64, @floatFromInt(@sizeOf(sched.Sched))) / 1048576.0 });
}
