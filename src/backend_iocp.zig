//! The IOCP half of the readiness reactor, for Windows.
//!
//! Same six functions as the other two backends, and this is the one where the
//! word "same" is doing real work, because IOCP is not a readiness interface.
//! epoll and kqueue answer "which sockets could you act on now". IOCP answers
//! "which operations that you already started have finished". Those are
//! different questions, and the ~390 lines of fairness policy in `reactor.zig`
//! are written against the first one.
//!
//! The bridge is the zero-byte overlapped operation. A `WSARecv` for zero bytes
//! completes when the socket becomes readable, without moving or consuming any
//! data. A `WSASend` for zero bytes pends while the send buffer is full and
//! completes when there is room. So "start an operation and wait for it to
//! finish" is made to answer "tell me when this is ready", and the reactor
//! above cannot tell the difference.
//!
//! This lines up with the reactor's oneshot design better than it has any right
//! to. Oneshot is something epoll and kqueue had to be asked for, with
//! `EPOLLONESHOT` and `EV_ONESHOT`. Here it is not a flag: one posted operation
//! produces exactly one completion, and nothing is registered afterwards. There
//! is no armed set to maintain at all.
//!
//! ## The part that is genuinely harder
//!
//! The kernel owns the `OVERLAPPED` block from the moment the operation is
//! posted until its completion packet has been dequeued, and cancelling does
//! not shorten that. `CancelIoEx` asks for the operation to stop; the packet
//! still arrives, marked `STATUS_CANCELLED`. So a cancelled operation's memory
//! cannot be reused at the moment of cancellation, only at the moment its
//! packet is collected, which is some later call to `wait`.
//!
//! That is why the blocks live in a pool here rather than one per task. An
//! orphaned block is one whose task has moved on but whose packet has not
//! arrived; it is held, not freed, and `wait` frees it when the packet lands
//! and wakes nobody. Getting this wrong would be a use-after-free that the
//! kernel performs on your behalf, at a time of its choosing, which is about
//! the worst shape a bug can have.
//!
//! The zero-byte trick makes this much less dangerous than it would otherwise
//! be. A completion reactor holding real buffers has the same problem with a
//! 16KB payload attached to every orphan. Here the only thing the kernel holds
//! is the 32-byte `OVERLAPPED` and nothing else, because there is no buffer.
//!
//! ## Two things deliberately not done
//!
//! `SetFileCompletionNotificationModes` with `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS`
//! is the usual optimisation here: an operation that succeeds immediately skips
//! posting a packet. It is not used, and must not be, because every count in
//! this file rests on one posted operation producing exactly one packet. With
//! that flag the count becomes "one packet, unless it was fast", and the pool
//! would leak a block for every fast operation.
//!
//! `Backend` must not be moved once armed. The kernel holds pointers into
//! `ops`, so copying the struct leaves those pointers aimed at the old copy.
//! The other two backends hold nothing but a descriptor and are move-safe; this
//! one is not. `reactor.Reactor` embeds it by value and the application holds
//! the reactor at a fixed address, which is what makes this fine in practice.

const std = @import("std");
const win = std.os.windows;
const ws2 = win.ws2_32;
const Interest = @import("interest.zig").Interest;

const sched = @import("sched.zig");
const sys_windows = @import("sys_windows.zig");

pub const TaskId = sched.TaskId;

/// Tasks index the tables below directly, so this tracks the scheduler's task
/// space rather than restating it.
const max_tasks = sched.max_tasks;

/// Two blocks per task. One is the live operation; the spare covers the window
/// where a task has cancelled an operation and armed another before the first
/// one's packet has come back. Exhaustion is handled rather than assumed away,
/// so this number is a comfort margin and not a correctness argument.
const max_ops = max_tasks * 2;

const SOCKET = usize;
const HANDLE = *anyopaque;
const WSA_IO_PENDING: c_int = 997;
const INFINITE: u32 = 0xFFFF_FFFF;

/// Creating a port and associating a socket with one are the same call, told
/// apart by this. A null handle is not the "no handle yet" value here, it is
/// simply invalid, which is a sharp edge worth naming rather than inlining.
const INVALID_HANDLE_VALUE: HANDLE = @ptrFromInt(std.math.maxInt(usize));

/// `AcceptEx` is not exported from ws2_32 like an ordinary function. It is
/// reached by asking a socket for a pointer to it, which is how Winsock ships
/// calls that belong to a particular provider rather than to the library.
const SIO_GET_EXTENSION_FUNCTION_POINTER: u32 = 0xc800_0006;
const SO_UPDATE_ACCEPT_CONTEXT: c_int = 0x700B;

const GUID = extern struct { d1: u32, d2: u16, d3: u16, d4: [8]u8 };

const WSAID_ACCEPTEX = GUID{
    .d1 = 0xb5367df1,
    .d2 = 0xcbac,
    .d3 = 0x11cf,
    .d4 = .{ 0x95, 0xca, 0x00, 0x80, 0x5f, 0x48, 0xa1, 0x92 },
};

const AcceptExFn = *const fn (
    sListenSocket: SOCKET,
    sAcceptSocket: SOCKET,
    lpOutputBuffer: *anyopaque,
    dwReceiveDataLength: u32,
    dwLocalAddressLength: u32,
    dwRemoteAddressLength: u32,
    lpdwBytesReceived: *u32,
    lpOverlapped: *OVERLAPPED,
) callconv(.winapi) win.BOOL;

/// `AcceptEx` writes both endpoint addresses into one buffer and demands at
/// least sixteen bytes of slack after each. A `sockaddr_in` is sixteen bytes,
/// so thirty-two each, sixty-four in total.
const addr_len: u32 = 32;
const accept_buf_len = addr_len * 2;

const OVERLAPPED = extern struct {
    Internal: usize = 0,
    InternalHigh: usize = 0,
    Offset: u32 = 0,
    OffsetHigh: u32 = 0,
    hEvent: ?*anyopaque = null,
};

const OVERLAPPED_ENTRY = extern struct {
    lpCompletionKey: usize,
    lpOverlapped: ?*OVERLAPPED,
    Internal: usize,
    dwNumberOfBytesTransferred: u32,
};

const WSABUF = extern struct {
    len: u32,
    buf: [*]u8,
};

const c = struct {
    extern "kernel32" fn CreateIoCompletionPort(
        FileHandle: ?HANDLE,
        ExistingCompletionPort: ?HANDLE,
        CompletionKey: usize,
        NumberOfConcurrentThreads: u32,
    ) callconv(.winapi) ?HANDLE;

    extern "kernel32" fn GetQueuedCompletionStatusEx(
        CompletionPort: HANDLE,
        lpCompletionPortEntries: [*]OVERLAPPED_ENTRY,
        ulCount: u32,
        ulNumEntriesRemoved: *u32,
        dwMilliseconds: u32,
        fAlertable: win.BOOL,
    ) callconv(.winapi) win.BOOL;

    extern "kernel32" fn CancelIoEx(
        hFile: HANDLE,
        lpOverlapped: ?*OVERLAPPED,
    ) callconv(.winapi) win.BOOL;

    extern "kernel32" fn CloseHandle(hObject: HANDLE) callconv(.winapi) win.BOOL;

    extern "ws2_32" fn WSARecv(
        s: SOCKET,
        lpBuffers: [*]WSABUF,
        dwBufferCount: u32,
        lpNumberOfBytesRecvd: ?*u32,
        lpFlags: *u32,
        lpOverlapped: ?*OVERLAPPED,
        lpCompletionRoutine: ?*const anyopaque,
    ) callconv(.winapi) c_int;

    extern "ws2_32" fn WSASend(
        s: SOCKET,
        lpBuffers: [*]WSABUF,
        dwBufferCount: u32,
        lpNumberOfBytesSent: ?*u32,
        dwFlags: u32,
        lpOverlapped: ?*OVERLAPPED,
        lpCompletionRoutine: ?*const anyopaque,
    ) callconv(.winapi) c_int;

    extern "ws2_32" fn WSAIoctl(
        s: SOCKET,
        dwIoControlCode: u32,
        lpvInBuffer: ?*const anyopaque,
        cbInBuffer: u32,
        lpvOutBuffer: ?*anyopaque,
        cbOutBuffer: u32,
        lpcbBytesReturned: *u32,
        lpOverlapped: ?*OVERLAPPED,
        lpCompletionRoutine: ?*const anyopaque,
    ) callconv(.winapi) c_int;

    extern "ws2_32" fn setsockopt(s: SOCKET, level: c_int, optname: c_int, optval: ?*const anyopaque, optlen: c_int) callconv(.winapi) c_int;
    extern "ws2_32" fn closesocket(s: SOCKET) callconv(.winapi) c_int;
    extern "ws2_32" fn WSAGetLastError() callconv(.winapi) c_int;
    extern "ws2_32" fn recv(s: SOCKET, buf: [*]u8, len: c_int, flags: c_int) callconv(.winapi) c_int;
};

/// What a block is doing, which is the whole of the lifetime problem in three
/// words.
///
/// `live` means a task is parked on it and its packet should wake that task.
/// `orphan` means the task has moved on but the kernel has not let go yet: its
/// packet must be collected and discarded, and the block freed only then.
const State = enum { free, live, orphan };

/// Which of the two arming mechanisms a block belongs to. `accept` blocks own a
/// socket and a buffer as well, so a cancelled one has more to give back.
const Kind = enum { ready, accept };

const Op = struct {
    ov: OVERLAPPED = .{},
    state: State = .free,
    kind: Kind = .ready,
    task: TaskId = 0,
    /// Kept so the block can be cancelled without asking the reactor which
    /// socket a task was on.
    fd: i32 = -1,
    /// `accept` only: the socket `AcceptEx` was told to accept into, and the
    /// buffer it writes the two addresses into. Both are held by the kernel for
    /// as long as the block is, which is the whole reason orphans exist.
    conn: i32 = -1,
    buf: [accept_buf_len]u8 = @splat(0),
};

pub const Backend = struct {
    port: ?HANDLE = null,

    ops: [max_ops]Op = @splat(.{}),
    /// Which block each task is parked on, or `no_op`.
    op_of: [max_tasks]u32 = @splat(no_op),
    /// The socket this task is currently associated with the port under.
    /// Association is per handle and permanent, so it is done once per socket
    /// and not once per arm. It is cleared on disarm because the descriptor
    /// number will be reused by a different socket later.
    assoc_fd: [max_tasks]i32 = @splat(-1),
    /// Next block to try, so allocation is a short scan from where the last one
    /// succeeded rather than from zero every time.
    hand: u32 = 0,
    /// Resolved once, from the first listening socket armed. Null until then.
    accept_ex: ?AcceptExFn = null,

    /// Readiness collected by a drain that was not `wait`. Only `allocOp` fills
    /// this, and only when the pool is empty; `wait` empties it before asking
    /// the kernel for more. Without it, a drain to reclaim orphans would throw
    /// away real wakeups and hang the tasks waiting on them.
    stash: [64]TaskId = @splat(0),
    stashed: usize = 0,

    const no_op: u32 = std.math.maxInt(u32);

    pub fn init(be: *Backend) !void {
        be.port = c.CreateIoCompletionPort(INVALID_HANDLE_VALUE, null, 0, 1) orelse
            return error.IocpCreateFailed;
    }

    pub fn deinit(be: *Backend) void {
        // Blocks the kernel still owns are not freed here, and cannot be:
        // closing the port does not retract them. The sockets are closed by the
        // layer above, which is what actually ends those operations.
        if (be.port) |p| _ = c.CloseHandle(p);
        be.port = null;
    }

    pub fn arm(be: *Backend, task: TaskId, fd: i32, i: Interest, rearm: bool) bool {
        _ = rearm; // Association is tracked per socket here, not per task.
        const port = be.port orelse return false;

        if (be.assoc_fd[task] != fd) {
            if (c.CreateIoCompletionPort(@ptrFromInt(@as(usize, @intCast(fd))), port, @intCast(task), 0) == null) return false;
            be.assoc_fd[task] = fd;
        }

        // A task that is already parked on a block is switching direction, so
        // the old operation has to go. It becomes an orphan rather than a free
        // block: its packet is still coming.
        if (be.op_of[task] != no_op) be.orphan(be.op_of[task]);

        const slot = be.allocOp() orelse return false;

        // A listener has no readiness to wait for, so it takes the other path
        // entirely. See `sys_windows.acceptNonblock` for what the server sees.
        if (sys_windows.isListening(fd)) return be.armAccept(slot, task, fd);

        const op = &be.ops[slot];
        op.* = .{ .state = .live, .task = task, .fd = fd };

        // Zero length, so no buffer is handed to the kernel at all. `buf` still
        // has to point somewhere valid even with `len` zero, so it points at
        // this file's own byte rather than at null.
        var wsabuf = WSABUF{ .len = 0, .buf = &zero_byte };
        const s: SOCKET = @intCast(fd);

        const rc = switch (i) {
            .read => blk: {
                var flags: u32 = 0;
                break :blk c.WSARecv(s, @ptrCast(&wsabuf), 1, null, &flags, &op.ov, null);
            },
            .write => c.WSASend(s, @ptrCast(&wsabuf), 1, null, 0, &op.ov, null),
        };

        // Zero means it completed immediately, and a packet is still posted for
        // it, so both outcomes are the same to everything below.
        //
        // A real failure means no packet is coming, and it is readiness rather
        // than an arming problem. The usual cause is a peer that sent RST
        // before the task got here, which leaves the socket in a state the task
        // must observe: it should run, read, and find the connection gone. So
        // the block is reclaimed and the task is reported ready at the next
        // `wait`, which is what epoll and kqueue do with a dead descriptor.
        //
        // Returning false here instead is what a first version did, and the
        // reactor takes that as "not armed": the task parks with nothing able
        // to wake it and stays there. Measured against 200 connections reset by
        // the peer, 2 were reclaimed and 198 were stranded.
        if (rc != 0 and c.WSAGetLastError() != WSA_IO_PENDING) {
            op.state = .free;
            return be.readyNow(task);
        }

        be.op_of[task] = slot;
        return true;
    }

    /// Post an `AcceptEx` on behalf of a task parked on a listener.
    ///
    /// The socket to accept into has to exist before the call, which is the
    /// inversion at the heart of the completion model: the kernel is not asked
    /// whether a client is there, it is handed the thing to do when one is.
    fn armAccept(be: *Backend, slot: u32, task: TaskId, fd: i32) bool {
        const accept_ex = be.resolveAcceptEx(fd) orelse return false;

        const conn = sys_windows.tcpSocketNonblock();
        if (@as(isize, @bitCast(conn)) < 0) return false;

        const op = &be.ops[slot];
        op.* = .{ .state = .live, .kind = .accept, .task = task, .fd = fd, .conn = @intCast(conn) };

        // A receive length of zero is what makes this an accept rather than an
        // accept-and-read. With a non-zero length the completion waits for the
        // client to send something, so a connection that opens and says nothing
        // would never be reported, and a client that opens connections and
        // stalls could hold every posted accept at once.
        var got: u32 = 0;
        const ok = accept_ex(
            @intCast(fd),
            @intCast(conn),
            &op.buf,
            0,
            addr_len,
            addr_len,
            &got,
            &op.ov,
        );

        if (ok == .FALSE and c.WSAGetLastError() != WSA_IO_PENDING) {
            _ = c.closesocket(conn);
            op.state = .free;
            return be.readyNow(task);
        }

        be.op_of[task] = slot;
        return true;
    }

    fn resolveAcceptEx(be: *Backend, fd: i32) ?AcceptExFn {
        if (be.accept_ex) |f| return f;
        var fptr: AcceptExFn = undefined;
        var got: u32 = 0;
        var guid = WSAID_ACCEPTEX;
        if (c.WSAIoctl(
            @intCast(fd),
            SIO_GET_EXTENSION_FUNCTION_POINTER,
            &guid,
            @sizeOf(GUID),
            @ptrCast(&fptr),
            @sizeOf(AcceptExFn),
            &got,
            null,
            null,
        ) != 0) return null;
        be.accept_ex = fptr;
        return fptr;
    }

    pub fn disarm(be: *Backend, task: TaskId, fd: i32) void {
        _ = fd;
        be.assoc_fd[task] = -1;
        const slot = be.op_of[task];
        if (slot == no_op) return;
        be.orphan(slot);
    }

    /// Detach a block from its task and ask the kernel to stop. The block stays
    /// allocated: the packet is still coming, and reusing the memory before it
    /// arrives is the use-after-free this whole file is arranged to avoid.
    fn orphan(be: *Backend, slot: u32) void {
        const op = &be.ops[slot];
        if (op.state != .live) return;
        be.op_of[op.task] = no_op;
        op.state = .orphan;
        if (op.fd >= 0) {
            // Failure here is expected and ignored. The usual reason is that
            // the operation already completed and its packet is in the queue,
            // which is the outcome being asked for anyway.
            _ = c.CancelIoEx(@ptrFromInt(@as(usize, @intCast(op.fd))), &op.ov);
        }
    }

    /// Report a task as ready without a completion packet behind it, for the
    /// case where the operation could not be started because the socket is
    /// already in the state the task needs to see. Counts as armed, because
    /// something will wake the task.
    fn readyNow(be: *Backend, task: TaskId) bool {
        if (be.stashed == be.stash.len) return false;
        be.stash[be.stashed] = task;
        be.stashed += 1;
        return true;
    }

    fn allocOp(be: *Backend) ?u32 {
        if (be.scan()) |s| return s;
        // Nothing free, which means orphans are holding the pool. Their packets
        // may already be queued, so collect what is there without blocking and
        // try once more. Real readiness picked up on the way is stashed rather
        // than dropped.
        be.drain(0);
        return be.scan();
    }

    fn scan(be: *Backend) ?u32 {
        var n: u32 = 0;
        while (n < max_ops) : (n += 1) {
            const s = (be.hand + n) % max_ops;
            if (be.ops[s].state == .free) {
                be.hand = (s + 1) % max_ops;
                return s;
            }
        }
        return null;
    }

    pub fn wait(be: *Backend, out: []TaskId, timeout_ms: i32) usize {
        var k: usize = 0;

        // Anything an emergency drain picked up is owed to its task before the
        // kernel is asked for more.
        while (be.stashed > 0 and k < out.len) {
            be.stashed -= 1;
            out[k] = be.stash[be.stashed];
            k += 1;
        }
        if (k > 0) return k;

        const ms: u32 = if (timeout_ms < 0) INFINITE else @intCast(timeout_ms);
        return be.collect(out, ms);
    }

    /// Pull packets and turn the live ones into task ids. Orphan packets are the
    /// point of the exercise: collecting one is what finally frees its block.
    fn collect(be: *Backend, out: []TaskId, ms: u32) usize {
        const port = be.port orelse return 0;
        var entries: [256]OVERLAPPED_ENTRY = undefined;
        const cap = @min(out.len, entries.len);
        if (cap == 0) return 0;

        var removed: u32 = 0;
        if (c.GetQueuedCompletionStatusEx(port, &entries, @intCast(cap), &removed, ms, .FALSE) == .FALSE) return 0;

        var k: usize = 0;
        for (entries[0..removed]) |e| {
            const ov = e.lpOverlapped orelse continue;
            const slot = be.slotOf(ov) orelse continue;
            const op = &be.ops[slot];
            switch (op.state) {
                .free => {},
                .orphan => {
                    // The kernel has finally let go, so now the block can be
                    // reused and, for an accept, the socket it was holding can
                    // be closed. Doing either of those at cancellation time
                    // would have been the bug this state exists to prevent.
                    if (op.kind == .accept and op.conn >= 0) _ = c.closesocket(@intCast(op.conn));
                    op.conn = -1;
                    op.state = .free;
                },
                .live => {
                    // Woken regardless of the completion status. A failed
                    // operation still means the task should look at its socket,
                    // and a peer hangup arriving as an error rather than as
                    // readability would otherwise be silently swallowed. The
                    // reactor's read returns 0 or an error either way and the
                    // existing peer_gone path handles it unchanged.
                    op.state = .free;
                    be.op_of[op.task] = no_op;
                    if (op.kind == .accept) be.finishAccept(op, e.Internal);
                    out[k] = op.task;
                    k += 1;
                },
            }
        }
        return k;
    }

    /// Collect without an `out` array, stashing live wakeups for the next
    /// `wait`. Used only to reclaim orphans when the pool is empty.
    fn drain(be: *Backend, ms: u32) void {
        var room: [64]TaskId = undefined;
        const space = room.len - be.stashed;
        if (space == 0) return;
        const n = be.collect(room[0..space], ms);
        for (room[0..n]) |t| {
            be.stash[be.stashed] = t;
            be.stashed += 1;
        }
    }

    /// Turn a completed `AcceptEx` into a socket the server can collect.
    ///
    /// Until `SO_UPDATE_ACCEPT_CONTEXT` is set, the accepted socket is in a
    /// half-made state: it is connected, but it has inherited none of the
    /// listener's properties, and calls like `getsockname` and `shutdown` fail
    /// on it. This is the step people leave out, and the symptom is a socket
    /// that works for reading and writing and then behaves strangely at
    /// teardown.
    ///
    /// The task is woken either way. A failed accept means the accept task
    /// should run and find a would-block, which is the same thing it would find
    /// on any other platform after a connection evaporated.
    fn finishAccept(be: *Backend, op: *Op, status: usize) void {
        _ = be;
        const listener = op.fd;
        const conn = op.conn;
        op.conn = -1;
        if (conn < 0) return;

        if (status != 0) {
            _ = c.closesocket(@intCast(conn));
            return;
        }

        const listener_sock: SOCKET = @intCast(listener);
        _ = c.setsockopt(@intCast(conn), 0xffff, SO_UPDATE_ACCEPT_CONTEXT, &listener_sock, @sizeOf(SOCKET));

        // Tagged with the listener, because this server listens twice and the
        // two are accepted by different tasks. A full queue means the server is
        // not collecting, so the honest thing is to refuse the connection
        // rather than hold it open unanswered.
        if (!sys_windows.pushAccepted(listener, conn)) _ = c.closesocket(@intCast(conn));
    }

    /// Which block a completed `OVERLAPPED` belongs to. Pointer arithmetic
    /// rather than a search: the kernel hands back the exact address that was
    /// posted, and it always points into `ops`.
    fn slotOf(be: *Backend, ov: *OVERLAPPED) ?u32 {
        const base = @intFromPtr(&be.ops[0]);
        const here = @intFromPtr(ov);
        if (here < base) return null;
        const off = here - base;
        const stride = @sizeOf(Op);
        if (off % stride != 0) return null;
        const idx = off / stride;
        if (idx >= max_ops) return null;
        return @intCast(idx);
    }

    pub fn read(fd: i32, buf: []u8) isize {
        // `recv`, not `ReadFile`. A socket handle is not a file handle on
        // Windows and the two calls are different code paths. This one is
        // synchronous and non-blocking, because the socket is in FIONBIO mode
        // and no overlapped operation is outstanding on it at this point: the
        // readiness operation completed, which is why the task is running.
        const n = c.recv(@intCast(fd), buf.ptr, @intCast(@min(buf.len, std.math.maxInt(c_int))), 0);
        return n;
    }
};

/// A valid address for a zero-length `WSABUF`. Never read or written; it exists
/// because the field may not be null even when the length is zero.
var zero_byte: [1]u8 = .{0};
