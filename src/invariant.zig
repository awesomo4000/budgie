//! One shape for "something that should be true is not".
//!
//! A leaf module, imported by anything that can check itself. It exists so a
//! scheduler violation and a pool violation are the same type, and so the
//! application can return whichever it found without translating.
//!
//! The name is a string rather than an enum on purpose. An enum would mean
//! adding a check in two places, and a check that is annoying to add is a
//! check nobody adds. The cost is that a typo in a name is not caught, which
//! matters less than the checks existing.
//!
//! What belongs here, and what does not. These are properties that hold at
//! every quiescent point regardless of workload: conservation, and states that
//! contradict each other. They are not assertions about a particular test's
//! expectations, and not counters of things that merely should not have
//! happened. `cancels_stale`, for instance, is not a violation: a token
//! outliving its task is exactly what the generation counter is for, and one
//! socket test provokes it deliberately.

const std = @import("std");

pub const Violation = struct {
    /// What was violated. Phrased as the thing that should be true.
    what: []const u8,
    /// Which task or slot, when the violation is about one.
    who: u32 = no_one,
    /// What was observed, and what should have been.
    got: i64 = 0,
    want: i64 = 0,

    pub const no_one: u32 = std.math.maxInt(u32);

    pub fn format(v: Violation, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        try writer.print("{s} (got {d}, want {d}", .{ v.what, v.got, v.want });
        if (v.who != no_one) try writer.print(", at {d}", .{v.who});
        try writer.writeAll(")");
    }
};
