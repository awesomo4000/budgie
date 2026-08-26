//! I/O buffers as a pool, not a per-connection field.
//!
//! A parked keep-alive connection needs no read buffer, no write buffer and no
//! parser state -- it is waiting for bytes that may never arrive. Embedding
//! them in `Conn` means paying for 4096 buffers to serve however many
//! connections actually have data in flight, which on this workload is a
//! small fraction.
//!
//! Handles are index + generation, so a stale handle from a released buffer
//! fails a check rather than aliasing whoever got the slot next. Same
//! discipline as the cancel token.
//!
//! Sizing the pool IS the memory budget: exhaustion is an admission decision
//! with a real answer (503), rather than an allocation failure. That is the
//! property a per-connection array cannot express, and it is why the store is
//! one mapping sized at startup: the number of buffers is a policy decision
//! the operator makes once, and everything downstream reads it off the pool.

const std = @import("std");
const http = @import("http.zig");

pub const max_bufs = 8192;
pub const nil: u16 = 0xffff;

pub const IoBuf = struct {
    /// Inbound accumulation.
    ///
    /// MUST be at least `uring_buf_size + max_request_bytes` so that a single
    /// completion always fits alongside a pending partial request. Otherwise
    /// the app has to refuse a completion whose bytes the kernel has already
    /// read, and there is no per-connection way to push back: a provided
    /// buffer ring is a GLOBAL resource, so holding a buffer throttles every
    /// connection rather than the greedy one. Multishot recv simply uses a
    /// different buffer for the same socket.
    ///
    /// With this sizing the only remaining overflow is genuine accumulation
    /// while the connection is busy -- a client sending faster than we serve,
    /// which is a resource limit (503) and not a protocol error (400).
    in: [16384]u8 = undefined,
    in_len: usize = 0,
    out: [256]u8 = undefined,
    out_len: usize = 0,
    out_sent: usize = 0,
    parser: http.Parser = .{},
};

pub const Handle = struct {
    idx: u16 = nil,
    gen: u32 = 0,

    pub fn isNull(h: Handle) bool {
        return h.idx == nil;
    }
};

/// Backing store for the pool, one anonymous mapping sized to the configured
/// pool rather than to `max_bufs`.
///
/// This was a `[max_bufs]IoBuf = undefined` in BSS, which read as free: the
/// pages stay unmapped until a slot is touched, so an unused ceiling cost
/// nothing resident. It is free at runtime and expensive everywhere else.
/// `max_bufs` slots is 135 MB of reserved address space whatever the pool is
/// actually set to, and in a Debug build it is far worse, because Zig writes
/// `0xAA` over `undefined` and that moves all 135 MB out of `.bss` and into
/// the binary. The linker then pads around it. A Debug server was a 498 MB
/// file for a pool that defaults to 64 buffers.
///
/// One mapping of exactly the slots asked for keeps the property that
/// mattered, since anonymous pages fault in on first touch the same way BSS
/// does, and drops the rest.
var store: []IoBuf = &.{};

/// Size the store. Idempotent, because the io_uring build registers the region
/// during reactor init and the pool initialises after it. Both are handed the
/// same `io_bufs`, so whichever runs first allocates.
///
/// Growing later is refused rather than reallocated: by then the region may be
/// registered with the kernel, and moving it would leave the ring pointing at
/// freed memory.
pub fn reserve(n_slots: usize) error{ OutOfMemory, StoreTooSmall }![]IoBuf {
    const n: usize = @min(n_slots, max_bufs);
    if (store.len == 0) store = try std.heap.page_allocator.alloc(IoBuf, n);
    if (n > store.len) return error.StoreTooSmall;
    return store;
}

/// The store as one region, so it can be handed to IORING_REGISTER_BUFFERS as
/// a single registered buffer. Every `out` buffer then lives inside registered
/// memory and a send can name an address within it, skipping get_user_pages
/// per operation.
///
/// Only the slots the pool will actually hand out. Registering a ceiling pins
/// every page of it, which is resident, pinned memory for slots that may never
/// be used.
pub fn storeRegion(n_slots: usize) ![]u8 {
    const n: usize = @min(n_slots, max_bufs);
    const s = try reserve(n);
    return std.mem.sliceAsBytes(s)[0 .. n * @sizeOf(IoBuf)];
}

pub const Pool = struct {
    gen: [max_bufs]u32 = @splat(1),
    free: [max_bufs]u16 = undefined,
    n_free: usize = 0,
    cap: usize = 0,

    live: usize = 0,
    high_water: usize = 0,
    acquires: u64 = 0,
    releases: u64 = 0,
    exhausted: u64 = 0,

    pub fn init(p: *Pool, cap: usize) !void {
        p.cap = @min(cap, @as(usize, max_bufs));
        _ = try reserve(p.cap);
        p.n_free = p.cap;
        var i: usize = 0;
        while (i < p.cap) : (i += 1) p.free[i] = @intCast(p.cap - 1 - i);
    }

    pub fn acquire(p: *Pool) ?Handle {
        if (p.n_free == 0) {
            p.exhausted += 1;
            return null;
        }
        p.n_free -= 1;
        const idx = p.free[p.n_free];
        p.live += 1;
        if (p.live > p.high_water) p.high_water = p.live;
        p.acquires += 1;
        // First touch of this slot is what maps its pages.
        store[idx] = .{};
        return .{ .idx = idx, .gen = p.gen[idx] };
    }

    pub fn release(p: *Pool, h: Handle) void {
        if (h.isNull() or p.gen[h.idx] != h.gen) return;
        p.gen[h.idx] +%= 1;
        if (p.gen[h.idx] == 0) p.gen[h.idx] = 1;
        p.free[p.n_free] = h.idx;
        p.n_free += 1;
        p.live -= 1;
        p.releases += 1;
    }

    pub fn get(p: *Pool, h: Handle) ?*IoBuf {
        if (h.isNull() or h.idx >= p.cap or p.gen[h.idx] != h.gen) return null;
        return &store[h.idx];
    }
};
