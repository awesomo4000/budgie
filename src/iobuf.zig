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
const Violation = @import("invariant.zig").Violation;

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
    /// Outbound. Large enough to hold a run of pipelined answers, so a
    /// client that sends N requests at once can be answered in one `write`
    /// instead of N. At ~63 bytes per answer this holds about 130 of them.
    out: [8192]u8 = undefined,
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

/// No owner recorded. Not a valid task id, and `releaseAllFor` will not match
/// it, so a slot taken through the un-owned `acquire` is never reclaimed by
/// somebody else's teardown.
pub const no_owner: u32 = std.math.maxInt(u32);

pub const Pool = struct {
    gen: [max_bufs]u32 = @splat(1),
    /// Who each live slot was handed to. Provenance, and the whole difference
    /// between reclaiming a resource and asking a task to hand it back.
    ///
    /// An unwind that forgets to release, or declines to, used to leak the
    /// buffer for the life of the process, and nothing could tell: the only
    /// evidence was the task failing to go away, and a task that reports
    /// itself finished takes even that away. seL4 has no such problem because
    /// objects are derived from untyped memory and the kernel wrote the
    /// derivation down, so `revoke` needs no cooperation from the holder.
    /// This is that idea at the smallest scale that works: one field per slot.
    ///
    /// The id is opaque here. The pool does not know what a task is, the same
    /// way the scheduler does not know what a buffer is. It holds an
    /// accounting structure, not a semantic one.
    owner: [max_bufs]u32 = @splat(no_owner),
    free: [max_bufs]u16 = undefined,
    n_free: usize = 0,
    cap: usize = 0,

    /// Slots withheld from `acquire`, reachable only by `acquireForCleanup`.
    /// One is enough: an unwind formats a short fixed response and releases
    /// immediately. A pool too small to spare one keeps none, since a pool of
    /// one that reserves its only slot can serve nobody.
    cleanup_slots: usize = 1,

    live: usize = 0,
    high_water: usize = 0,
    acquires: u64 = 0,
    releases: u64 = 0,
    exhausted: u64 = 0,

    pub fn init(p: *Pool, cap: usize) !void {
        p.cap = @min(cap, @as(usize, max_bufs));
        p.cleanup_slots = if (p.cap >= 2) 1 else 0;
        _ = try reserve(p.cap);
        p.n_free = p.cap;
        var i: usize = 0;
        while (i < p.cap) : (i += 1) p.free[i] = @intCast(p.cap - 1 - i);
    }

    /// Take a buffer for a request body. Stops one short of empty, so the
    /// last slot stays available to an unwind.
    ///
    /// Refusing here is the admission decision the pool exists to make: the
    /// caller answers 503 rather than failing to allocate. That answer has to
    /// be written somewhere, which is what `cleanup_slots` is for.
    /// Take a buffer, recording who it went to.
    ///
    /// There is no un-owned version. There was, kept when provenance was added
    /// so the servers could adopt it one at a time, and once both had it the
    /// old one had no callers and every reason to have none: a slot taken
    /// without an owner is one `releaseAllFor` can never reclaim, which is
    /// exactly the leak the ledger exists to prevent. Leaving it there was
    /// leaving the footgun loaded and pointing at the next person to write an
    /// acquire.
    pub fn acquireFor(p: *Pool, owner: u32) ?Handle {
        if (p.n_free <= p.cleanup_slots) {
            p.exhausted += 1;
            return null;
        }
        return p.take(owner);
    }

    /// Take a buffer for an unwind, including the reserved one.
    ///
    /// The execution budget keeps a cleanup reserve the body cannot spend, so
    /// an unwind is funded even when the work that overran it was not. This is
    /// the same idea for memory. Without it, a pool that runs dry takes the
    /// error response with it: the body fails to acquire, the unwind retries
    /// the identical call, fails identically, and the connection closes with
    /// nothing written. Measured before this existed: 48 connections against a
    /// pool of 2 produced 42 `no_buffer` endings and zero delivered 503s.
    pub fn acquireForCleanupBy(p: *Pool, owner: u32) ?Handle {
        if (p.n_free == 0) {
            p.exhausted += 1;
            return null;
        }
        return p.take(owner);
    }

    fn take(p: *Pool, owner: u32) ?Handle {
        p.n_free -= 1;
        const idx = p.free[p.n_free];
        p.owner[idx] = owner;
        p.live += 1;
        if (p.live > p.high_water) p.high_water = p.live;
        p.acquires += 1;
        // First touch of this slot is what maps its pages.
        store[idx] = .{};
        return .{ .idx = idx, .gen = p.gen[idx] };
    }

    pub fn release(p: *Pool, h: Handle) void {
        if (h.isNull() or p.gen[h.idx] != h.gen) return;
        p.owner[h.idx] = no_owner;
        p.gen[h.idx] +%= 1;
        if (p.gen[h.idx] == 0) p.gen[h.idx] = 1;
        p.free[p.n_free] = h.idx;
        p.n_free += 1;
        p.live -= 1;
        p.releases += 1;
    }

    /// Take back everything this owner still holds, and say how much that was.
    ///
    /// Called on teardown whatever the task claims to have done, so a unwind
    /// that releases nothing costs a missed courtesy rather than a leak. A
    /// non-zero return is a task that ended while still holding something,
    /// which is worth reporting: the reclaim already happened, but somebody
    /// wrote an unwind that does not do its job.
    ///
    /// O(cap), on the teardown path only. A task holds at most one buffer
    /// today, so a per-owner index would make this O(1); that is worth doing
    /// if a connection-churn benchmark ever shows the walk, and not before.
    pub fn releaseAllFor(p: *Pool, owner: u32) usize {
        if (owner == no_owner or p.live == 0) return 0;
        var n: usize = 0;
        var i: usize = 0;
        while (i < p.cap) : (i += 1) {
            if (p.owner[i] != owner) continue;
            p.release(.{ .idx = @intCast(i), .gen = p.gen[i] });
            n += 1;
        }
        return n;
    }

    /// What the pool can say about itself. Conservation, mostly: every slot is
    /// either free or handed out, and the counters agree with the state.
    ///
    /// `acquires == releases + live` is the one that has caught things before.
    /// It is the check the socket tests already make by hand, here so a caller
    /// gets it without knowing to ask.
    pub fn check(p: *const Pool) ?Violation {
        if (p.n_free + p.live != p.cap)
            return .{ .what = "every slot is free or live", .got = @intCast(p.n_free + p.live), .want = @intCast(p.cap) };
        if (p.acquires != p.releases + p.live)
            return .{ .what = "every buffer acquired is live or released", .got = @intCast(p.acquires), .want = @intCast(p.releases + p.live) };

        // An owner recorded against a slot that is on the free list means the
        // provenance record and the free list disagree, which is how a
        // reclaim could hand back something already reclaimed.
        var owned: usize = 0;
        var i: usize = 0;
        while (i < p.cap) : (i += 1) {
            if (p.owner[i] != no_owner) owned += 1;
        }
        if (owned > p.live)
            return .{ .what = "no more owners recorded than live slots", .got = @intCast(owned), .want = @intCast(p.live) };
        return null;
    }

    pub fn get(p: *Pool, h: Handle) ?*IoBuf {
        if (h.isNull() or h.idx >= p.cap or p.gen[h.idx] != h.gen) return null;
        return &store[h.idx];
    }
};
