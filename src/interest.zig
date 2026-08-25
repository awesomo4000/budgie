//! What a task is waiting for.
//!
//! This lives in its own file so that `reactor.zig` and the platform backends
//! can both name it without importing each other. It is deliberately the
//! smallest possible vocabulary: one fd, one direction, one wakeup. Anything
//! richer belongs to the reactor's policy, not to the wakeup primitive.

pub const Interest = enum { read, write };
