//! Where io_uring earns its reputation: many small I/O operations where the
//! syscall IS the cost. N random 4K reads over a file, three ways.
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
    var argv: [5][]const u8 = undefined; var argc: usize = 0;
    for (init.args.vector) |a| { if (argc == 5) break; argv[argc] = std.mem.span(a); argc += 1; }
    const n_ops: usize = if (argc > 1) try std.fmt.parseInt(usize, argv[1], 10) else 200_000;
    const file_mb: usize = 64;
    const gpa = std.heap.page_allocator;

    // build the file
    const path = "/home/claude/iobench.dat";
    {
        const fd = linux.open(path, .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true }, 0o644);
        const buf = try gpa.alloc(u8, 1 << 20);
        defer gpa.free(buf);
        @memset(buf, 0xab);
        var i: usize = 0;
        while (i < file_mb) : (i += 1) _ = linux.write(@intCast(fd), buf.ptr, buf.len);
        _ = linux.close(@intCast(fd));
    }
    const rfd_r = linux.open(path, .{ .ACCMODE = .RDONLY }, 0);
    const fd: i32 = @intCast(rfd_r);

    var prng = std.Random.DefaultPrng.init(1);
    const rnd = prng.random();
    const max_off: u64 = file_mb * (1 << 20) - block;
    const offs = try gpa.alloc(u64, n_ops);
    for (offs) |*o| o.* = (rnd.uintLessThan(u64, max_off) / block) * block;

    var buf: [block]u8 = undefined;
    var sink: u64 = 0;

    // --- 1. pread loop: one syscall per read ---
    var t0 = nowNs();
    for (offs) |o| {
        const rc = linux.pread(fd, &buf, block, @intCast(o));
        if (!sysErr(rc)) sink +%= buf[0];
    }
    const t_pread = nowNs() - t0;

    // --- 2. io_uring, depth 1: still one enter per read ---
    var ring1 = try IoUring.init(64, 0);
    const bufs = try gpa.alloc([block]u8, 256);
    t0 = nowNs();
    for (offs) |o| {
        _ = try ring1.read(0, fd, .{ .buffer = &bufs[0] }, o);
        _ = try ring1.submit_and_wait(1);
        var c: [4]linux.io_uring_cqe = undefined;
        _ = try ring1.copy_cqes(&c, 0);
        sink +%= bufs[0][0];
    }
    const t_u1 = nowNs() - t0;

    // --- 3. io_uring, depth D: D reads per syscall ---
    const results = try gpa.alloc(i64, 4);
    const depths = [_]u32{ 8, 32, 128, 256 };
    for (depths, 0..) |D, di| {
        var ring = try IoUring.init(1024, 0);
        t0 = nowNs();
        var i: usize = 0;
        while (i < n_ops) {
            const batch = @min(@as(usize, D), n_ops - i);
            var k: usize = 0;
            while (k < batch) : (k += 1) _ = try ring.read(@intCast(k), fd, .{ .buffer = &bufs[k % 256] }, offs[i + k]);
            _ = try ring.submit_and_wait(@intCast(batch));
            var c: [512]linux.io_uring_cqe = undefined;
            var got: usize = 0;
            while (got < batch) got += try ring.copy_cqes(&c, 0);
            i += batch;
        }
        results[di] = nowNs() - t0;
        ring.deinit();
    }

    const rate = struct {
        fn f(ns: i64, n: usize) f64 {
            return @as(f64, @floatFromInt(n)) / (@as(f64, @floatFromInt(ns)) / 1e9);
        }
    }.f;
    std.debug.print("\n{d} random 4K reads over a {d}MB file (page-cached: this measures\nthe per-operation cost, which is what io_uring attacks)\n\n", .{ n_ops, file_mb });
    std.debug.print("  {s:<26} {s:>12} {s:>10}\n", .{ "method", "ops/sec", "vs pread" });
    std.debug.print("  {s:<26} {d:>12.0} {s:>10}\n", .{ "pread (1 syscall/op)", rate(t_pread, n_ops), "1.00x" });
    std.debug.print("  {s:<26} {d:>12.0} {d:>9.2}x\n", .{ "io_uring depth 1", rate(t_u1, n_ops), rate(t_u1, n_ops) / rate(t_pread, n_ops) });
    for (depths, 0..) |D, di| {
        var name: [32]u8 = undefined;
        const nm = try std.fmt.bufPrint(&name, "io_uring depth {d}", .{D});
        std.debug.print("  {s:<26} {d:>12.0} {d:>9.2}x\n", .{ nm, rate(results[di], n_ops), rate(results[di], n_ops) / rate(t_pread, n_ops) });
    }
    std.debug.print("\n  (sink {d})\n", .{sink});
    _ = linux.close(fd);
}
