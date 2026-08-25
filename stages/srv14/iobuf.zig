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
//! with a real answer (503), not an allocation failure. That is the property
//! a per-connection array cannot express.

const std = @import("std");
const http = @import("http.zig");

pub const max_bufs = 8192;
pub const nil: u16 = 0xffff;

pub const IoBuf = struct {
    /// Holds the pipelined remainder of a recv while one request is being
    /// served. Sized for a deep pipeline of small requests.
    in: [2048]u8 = undefined,
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

/// Storage lives in BSS and is never initialised as a whole: pages stay
/// unmapped until a slot is actually used. Writing `@splat(.{})` over this
/// array would touch every page and defeat the entire point.
var store: [max_bufs]IoBuf = undefined;

/// The whole store as one region, so it can be handed to
/// IORING_REGISTER_BUFFERS as a single registered buffer. Every `out` buffer
/// then lives inside registered memory and a send can name an address within
/// it, skipping get_user_pages per operation.
/// Only the slots the pool will actually hand out. Registering the whole
/// `max_bufs` array pins every page of it -- 8192 x 2904B = 23.8 MB of
/// resident, pinned memory for a pool that may only ever use 32 slots. The
/// registered region must match the pool's cap, not its ceiling.
pub fn storeRegion(n_slots: usize) []u8 {
    const n = @min(n_slots, max_bufs);
    return std.mem.asBytes(&store)[0 .. n * @sizeOf(IoBuf)];
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

    pub fn init(p: *Pool, cap: usize) void {
        p.cap = @min(cap, max_bufs);
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
