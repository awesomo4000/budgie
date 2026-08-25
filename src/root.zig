//! coopkernel — a single-threaded cooperative kernel in userspace.
//!
//! This is the library root. It re-exports the ten files that make up the
//! kernel proper; consumers reach them as `@import("coopkernel").sched` and so
//! on. Inside this directory the files still import each other by path, which
//! keeps the dependency graph visible at the top of each file.
//!
//! The dependency graph is a DAG and every edge is listed here:
//!
//!     quota   <- sched <- drr, reactor, reactor_uring, reactor_uring2
//!     http    <- iobuf <- reactor_uring2
//!     accounts, clock  (leaves)
//!
//! `sched` is the only file with no I/O in it. The readiness reactor runs on
//! epoll or kqueue; the io_uring reactors are Linux-only and gated as such.

const builtin = @import("builtin");

/// Scheduler: priority classes, timer wheel, generational cancellation, and
/// one opaque execution budget per task. Knows nothing of fds or protocols.
pub const sched = @import("sched.zig");

/// Conservation trees over a resource. Generic over the unit; a draw deducts
/// from every ancestor, all-or-nothing.
pub const quota = @import("quota.zig");

/// Per-phase telemetry with application-defined labels.
pub const accounts = @import("accounts.zig");

/// Real or virtual time, injected. The simulator swaps this out.
pub const clock = @import("clock.zig");

/// Sockets, the tick timer, and process introspection. The platform seam for
/// the application, as `reactor.backend` is for the wakeup primitive: a
/// selector over `sys_linux.zig` and `sys_darwin.zig`.
pub const sys = @import("sys.zig");

/// Sans-I/O HTTP request parser: bytes in, events out.
pub const http = @import("http.zig");

/// Generational pool of I/O buffers.
pub const iobuf = @import("iobuf.zig");

/// Deficit round robin over byte flows, enforced by pausing a recv.
pub const drr = @import("drr.zig");

/// Readiness reactor: epoll on Linux, kqueue on macOS and the BSDs. The
/// platform seam is `reactor.backend`; the fairness policy above it is shared.
pub const reactor = @import("reactor.zig");

/// What a task waits for. Named separately so the reactor and its backends can
/// both reach it without importing each other.
pub const interest = @import("interest.zig");

/// The io_uring reactors, which exist only on Linux. There is no macOS or BSD
/// equivalent to port them to: kqueue is a readiness interface, and the whole
/// point of `reactor_uring2` is completion with a kernel-owned buffer ring.
/// They are gated rather than merely unreferenced so that naming one on a
/// platform that cannot have it is a clear error and not a wall of io_uring
/// internals from inside the standard library.
pub const has_io_uring = builtin.os.tag == .linux;

/// io_uring reactor via IORING_OP_POLL_ADD (readiness). Linux only.
pub const reactor_uring = if (has_io_uring)
    @import("reactor_uring.zig")
else
    @compileError("reactor_uring requires Linux io_uring");

/// io_uring reactor via multishot recv and a provided buffer ring
/// (completion). Linux 5.19+, 6.0+ for IORING_RECV_MULTISHOT.
pub const reactor_uring2 = if (has_io_uring)
    @import("reactor_uring2.zig")
else
    @compileError("reactor_uring2 requires Linux io_uring");
