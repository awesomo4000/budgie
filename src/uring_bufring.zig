//! Registering a provided buffer ring, around a broken kernel.
//!
//! This is a compatibility shim, not a temporary patch: it stays as long as
//! affected kernels are still in circulation, which is not something the
//! program can know from here. It costs one extra `io_uring_register` at
//! startup on a broken kernel and nothing at all on a working one, so
//! carrying it is close to free. It is kept in its own file so that retiring
//! it, when that day comes, is a single `git rm` and one call site.
//!
//! Ubuntu 6.8.0-136 and -137 return `EINVAL` from `io_register_pbuf_ring`
//! when the `io_uring_buf_reg` reserved fields are correctly zeroed, and
//! succeed when they are not. The check is inverted, so every spec-compliant
//! caller -- liburing and Zig's standard library included, precisely because
//! they zero the field -- cannot register a buffer ring at all. That takes
//! `server_uring2` and `bench/gen.zig` with it.
//!
//! Launchpad #2162843. The cause is a mangled backport of upstream
//! `1724849` ("io_uring/kbuf: use mem_is_zero()"), which rewrote
//!
//!     if (reg.resv[0] || reg.resv[1] || reg.resv[2])   return -EINVAL;
//!
//! as `if (!mem_is_zero(reg.resv, sizeof(reg.resv)))`. `mem_is_zero` is
//! `memchr_inv(s, 0, n) == NULL`, so upstream compiles to a `jne` into the
//! error path; the shipped kernel has a `je`. The negation was lost.
//!
//! The shape is what makes it safe to ship anywhere: register the correct way
//! first, and fall back only on `EINVAL`. A correct kernel succeeds on the
//! first call and never reaches the second, so no correct system ever sees a
//! non-zero reserved field from this program. Only a kernel carrying the bug
//! gets the deliberately-wrong registration, which is the only thing it
//! accepts. If both fail the error is real and is returned.

const std = @import("std");
const linux = std.os.linux;

/// Set once if the fallback was needed, so the server can say so in its stats
/// rather than silently running against a kernel with a known bug.
pub var used_workaround = false;

pub const Error = error{ BufRingRegisterFailed, OutOfMemory } || std.posix.MMapError;

/// Allocate and register a provided buffer ring. Same contract as
/// `IoUring.setup_buf_ring`, which this replaces only in order to reach the
/// raw registration struct.
pub fn setup(fd: i32, entries: u16, bgid: u16) Error!*align(std.heap.page_size_min) linux.io_uring_buf_ring {
    if (entries == 0 or !std.math.isPowerOfTwo(entries)) return error.BufRingRegisterFailed;

    const bytes = @as(usize, entries) * @sizeOf(linux.io_uring_buf);
    const mem = try std.posix.mmap(
        null,
        bytes,
        .{ .READ = true, .WRITE = true },
        .{ .TYPE = .PRIVATE, .ANONYMOUS = true },
        -1,
        0,
    );
    errdefer std.posix.munmap(mem);
    const br: *align(std.heap.page_size_min) linux.io_uring_buf_ring = @ptrCast(mem.ptr);

    // Built by hand rather than through the struct, so that the reserved
    // fields are addressable. Layout is checked below.
    var reg: [40]u8 align(8) = @splat(0);
    std.mem.writeInt(u64, reg[0..8], @intFromPtr(mem.ptr), .little);
    std.mem.writeInt(u32, reg[8..12], entries, .little);
    std.mem.writeInt(u16, reg[12..14], bgid, .little);
    // reg[14..16] flags = 0, reg[16..40] resv = 0

    switch (linux.errno(register(fd, &reg))) {
        .SUCCESS => return br,
        // Only EINVAL can be the inverted check. Anything else -- EPERM from a
        // lockdown, ENOMEM, EEXIST from a duplicate group -- is a real failure
        // and retrying with a corrupt struct would only obscure it.
        .INVAL => {},
        else => return error.BufRingRegisterFailed,
    }

    std.mem.writeInt(u64, reg[16..24], 1, .little); // resv[0] = 1
    if (linux.errno(register(fd, &reg)) != .SUCCESS) return error.BufRingRegisterFailed;

    used_workaround = true;
    return br;
}

fn register(fd: i32, reg: *const [40]u8) usize {
    return linux.io_uring_register(fd, .REGISTER_PBUF_RING, @ptrCast(reg), 1);
}

comptime {
    // The hand-built struct above is only correct if this is the real layout.
    const R = linux.io_uring_buf_reg;
    std.debug.assert(@sizeOf(R) == 40);
    std.debug.assert(@offsetOf(R, "ring_addr") == 0);
    std.debug.assert(@offsetOf(R, "ring_entries") == 8);
    std.debug.assert(@offsetOf(R, "bgid") == 12);
    std.debug.assert(@offsetOf(R, "flags") == 14);
    std.debug.assert(@offsetOf(R, "resv") == 16);
}

test "rejects entry counts the kernel would reject anyway" {
    try std.testing.expectError(error.BufRingRegisterFailed, setup(-1, 0, 0));
    try std.testing.expectError(error.BufRingRegisterFailed, setup(-1, 3, 0));
}
