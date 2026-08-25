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
//! `sched` is the only file with no I/O in it. `reactor*` are Linux-only.

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

/// Sans-I/O HTTP request parser: bytes in, events out.
pub const http = @import("http.zig");

/// Generational pool of I/O buffers.
pub const iobuf = @import("iobuf.zig");

/// Deficit round robin over byte flows, enforced by pausing a recv.
pub const drr = @import("drr.zig");

/// epoll reactor (readiness). Linux only.
pub const reactor = @import("reactor.zig");

/// io_uring reactor via IORING_OP_POLL_ADD (readiness). Linux only.
pub const reactor_uring = @import("reactor_uring.zig");

/// io_uring reactor via multishot recv and a provided buffer ring
/// (completion). Linux 5.19+, 6.0+ for IORING_RECV_MULTISHOT.
pub const reactor_uring2 = @import("reactor_uring2.zig");
