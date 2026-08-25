const std = @import("std");
const linux = std.os.linux;
fn nowNs() i64 { var ts: linux.timespec = undefined; _ = linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64,@intCast(ts.sec))*1_000_000_000 + @as(i64,@intCast(ts.nsec)); }
pub fn main() !void {
    const N: usize = 2_000_000;
    var sink: usize = 0;
    // cheapest possible syscall: getpid (no vDSO shortcut when called raw)
    var t0 = nowNs();
    var i: usize = 0;
    while (i < N) : (i += 1) sink +%= linux.syscall0(.getpid);
    const t_getpid = nowNs() - t0;
    // a real but trivial I/O syscall: read 0 bytes from /dev/null
    const fd: i32 = @intCast(linux.open("/dev/null", .{ .ACCMODE = .RDONLY }, 0));
    var b: [1]u8 = undefined;
    t0 = nowNs();
    i = 0;
    while (i < N) : (i += 1) sink +%= linux.read(fd, &b, 0);
    const t_read = nowNs() - t0;
    std.debug.print("getpid : {d:.0} ns/syscall\nread0  : {d:.0} ns/syscall\n(sink {d})\n", .{
        @as(f64,@floatFromInt(t_getpid))/@as(f64,@floatFromInt(N)),
        @as(f64,@floatFromInt(t_read))/@as(f64,@floatFromInt(N)), sink });
}
