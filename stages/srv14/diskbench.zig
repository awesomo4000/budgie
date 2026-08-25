//! io_uring on its actual home turf: real device I/O with queue depth.
//! O_DIRECT bypasses the page cache, so each read costs real device latency
//! and the only way to go faster is to have several in flight at once --
//! which is what io_uring does and a pread loop structurally cannot.
const std = @import("std");
const linux = std.os.linux;
const IoUring = linux.IoUring;

fn nowNs() i64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, @intCast(ts.sec)) * 1_000_000_000 + @as(i64, @intCast(ts.nsec));
}
fn sysErr(rc: usize) bool { return @as(isize, @bitCast(rc)) < 0; }

const block = 4096;

pub fn main(init: std.process.Init.Minimal) !void {
    var argv: [4][]const u8 = undefined; var argc: usize = 0;
    for (init.args.vector) |a| { if (argc == 4) break; argv[argc] = std.mem.span(a); argc += 1; }
    const n_ops: usize = if (argc > 1) try std.fmt.parseInt(usize, argv[1], 10) else 20000;
    const file_mb: usize = 512;
    const gpa = std.heap.page_allocator;
    const path = "/home/claude/direct.dat";

    {
        const fd = linux.open(path, .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true }, 0o644);
        const buf = try gpa.alignedAlloc(u8, .fromByteUnits(4096), 1 << 20);
        @memset(buf, 0xcd);
        for (0..file_mb) |_| _ = linux.write(@intCast(fd), buf.ptr, buf.len);
        _ = linux.fsync(@intCast(fd));
        _ = linux.close(@intCast(fd));
        gpa.free(buf);
    }
    // Drop what we can from cache, then open O_DIRECT so reads hit the device.
    const O_DIRECT: u32 = 0o40000;
    const rc = linux.syscall3(.open, @intFromPtr(path), @as(usize, 0) | O_DIRECT, 0);
    if (sysErr(rc)) return error.OpenDirect;
    const fd: i32 = @intCast(rc);

    var prng = std.Random.DefaultPrng.init(7);
    const rnd = prng.random();
    const max_off: u64 = file_mb * (1 << 20) - block;
    const offs = try gpa.alloc(u64, n_ops);
    for (offs) |*o| o.* = (rnd.uintLessThan(u64, max_off) / block) * block;

    const rate = struct {
        fn f(ns: i64, n: usize) f64 { return @as(f64, @floatFromInt(n)) / (@as(f64, @floatFromInt(ns)) / 1e9); }
    }.f;

    std.debug.print("\n{d} random 4K O_DIRECT reads over a {d}MB file (real device I/O)\n\n", .{ n_ops, file_mb });
    std.debug.print("  {s:<28} {s:>12} {s:>12} {s:>10}\n", .{ "method", "IOPS", "avg latency", "vs pread" });

    // --- serial pread ---
    const buf1 = try gpa.alignedAlloc(u8, .fromByteUnits(4096), block);
    var t0 = nowNs();
    var sink: u64 = 0;
    for (offs) |o| {
        const r = linux.pread(fd, buf1.ptr, block, @intCast(o));
        if (!sysErr(r)) sink +%= buf1[0];
    }
    const t_pread = nowNs() - t0;
    const base = rate(t_pread, n_ops);
    std.debug.print("  {s:<28} {d:>12.0} {d:>10.1}us {s:>10}\n", .{ "pread, 1 in flight", base, @as(f64, @floatFromInt(t_pread)) / @as(f64, @floatFromInt(n_ops)) / 1000.0, "1.00x" });

    const depths = [_]u32{ 1, 4, 16, 64, 128 };
    const bufs = try gpa.alignedAlloc(u8, .fromByteUnits(4096), block * 256);
    for (depths) |D| {
        var ring = try IoUring.init(512, 0);
        defer ring.deinit();
        t0 = nowNs();
        var i: usize = 0;
        while (i < n_ops) {
            const batch = @min(@as(usize, D), n_ops - i);
            for (0..batch) |k| _ = try ring.read(@intCast(k), fd, .{ .buffer = bufs[(k % 256) * block ..][0..block] }, offs[i + k]);
            _ = try ring.submit_and_wait(@intCast(batch));
            var c: [256]linux.io_uring_cqe = undefined;
            var got: usize = 0;
            while (got < batch) got += try ring.copy_cqes(&c, 0);
            i += batch;
        }
        const t = nowNs() - t0;
        var nm: [40]u8 = undefined;
        const s = try std.fmt.bufPrint(&nm, "io_uring, {d} in flight", .{D});
        std.debug.print("  {s:<28} {d:>12.0} {d:>10.1}us {d:>9.2}x\n", .{ s, rate(t, n_ops), @as(f64, @floatFromInt(t)) / @as(f64, @floatFromInt(n_ops)) / 1000.0, rate(t, n_ops) / base });
    }
    std.debug.print("\n  (sink {d})\n", .{sink});
    _ = linux.close(fd);
}
