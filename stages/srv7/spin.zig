const std = @import("std");
const linux = std.os.linux;
var sink: u64 = 0;
pub fn main(init: std.process.Init.Minimal) !void {
    var argv: [4][]const u8 = undefined; var argc: usize = 0;
    for (init.args.vector) |a| { if (argc==4) break; argv[argc]=std.mem.span(a); argc+=1; }
    const secs: i64 = if (argc>1) (std.fmt.parseInt(i64,argv[1],10) catch 5) else 5;
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    const stop = ts.sec + secs;
    var iters: u64 = 0;
    while (true) {
        var j: u32 = 0;
        while (j < 200000) : (j += 1) sink = sink *% 6364136223846793005 +% 1442695040888963407;
        iters += 1;
        _ = linux.clock_gettime(.MONOTONIC, &ts);
        if (ts.sec >= stop) break;
    }
    std.debug.print("spinner: {d} iters in {d}s\n", .{ iters, secs });
}
