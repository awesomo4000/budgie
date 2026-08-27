const std = @import("std");

/// The two servers. Same scheduler, different reactor.
///
/// `server` runs anywhere the readiness reactor does -- epoll on Linux, kqueue
/// on macOS and the BSDs. `server_uring2` is Linux-only and there is nothing
/// to port it to: kqueue is a readiness interface, and that build exists to
/// exercise completion with a kernel-owned buffer ring.
const apps = [_][]const u8{ "server", "server_uring2" };
const linux_only_apps = [_][]const u8{"server_uring2"};

/// Load generators and microbenchmarks. Every one is standalone -- they import
/// only `std`, so a bench can be built and shipped to a load box on its own.
///
/// All of them still speak raw Linux syscalls directly and have not been
/// ported; `gen` and `diskbench` are io_uring and never will be. The bench
/// step is gated on Linux for that reason.
const benches = [_][]const u8{
    "gen",       "bench",  "bench2",  "ctrl",
    "diskbench", "hold",   "iobench", "sysc",
};

/// Test programs, with the arguments `zig build test` runs them under.
/// These are ordinary executables with a `main`, not `test` blocks: the fuzz
/// and simulation drivers take a seed and an iteration count, and being able
/// to re-run one by hand with a different seed is the point of them.
const Test = struct {
    name: []const u8,
    args: []const []const u8 = &.{},
    /// sim is a determinism check rather than an assertion check, and it runs
    /// six hours of virtual time; it wants the fast build.
    fast: bool = false,
    /// echo_test starts the real example server, so it needs it as a module.
    needs_echo: bool = false,
};
const tests = [_]Test{
    .{ .name = "parser_test" },
    .{ .name = "pipetest" },
    .{ .name = "cancel_test" },
    .{ .name = "feedcmp" },
    .{ .name = "chunkfuzz" },
    .{ .name = "sim", .args = &.{ "42", "64", "30" }, .fast = true },
    .{ .name = "echo_test", .needs_echo = true },
};

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{ .preferred_optimize_mode = .ReleaseFast });

    // Tests default to ReleaseSafe and not to `optimize`. The suite leans on
    // `std.debug.assert` for its invariants, and ReleaseFast compiles those
    // out — a green run in ReleaseFast would be checking almost nothing.
    const test_optimize = b.option(
        std.builtin.OptimizeMode,
        "test-optimize",
        "Optimize mode for the test programs (default: ReleaseSafe)",
    ) orelse .ReleaseSafe;

    const budgie = b.addModule("budgie", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const is_linux = target.result.os.tag == .linux;

    for (apps) |name| {
        if (!is_linux and isLinuxOnly(name)) continue;
        const exe = b.addExecutable(.{
            .name = name,
            .root_module = b.createModule(.{
                .root_source_file = b.path(b.fmt("app/{s}.zig", .{name})),
                .target = target,
                .optimize = optimize,
                .imports = &.{.{ .name = "budgie", .module = budgie }},
            }),
        });
        b.installArtifact(exe);

        const run = b.addRunArtifact(exe);
        if (b.args) |args| run.addArgs(args);
        b.step(name, b.fmt("Run {s}", .{name})).dependOn(&run.step);
    }

    // The example is built by `zig build` like anything else, so it cannot rot
    // into pseudocode while nobody is looking.
    {
        const exe = b.addExecutable(.{
            .name = "echo",
            .root_module = b.createModule(.{
                .root_source_file = b.path("examples/echo.zig"),
                .target = target,
                .optimize = optimize,
                .imports = &.{.{ .name = "budgie", .module = budgie }},
            }),
        });
        b.installArtifact(exe);
        const run = b.addRunArtifact(exe);
        run.stdio = .inherit;
        if (b.args) |args| run.addArgs(args);
        b.step("echo", "Run the example server from examples/echo.zig").dependOn(&run.step);
    }

    const bench_step = b.step("bench", "Build the load generators and microbenchmarks");
    if (!is_linux) {
        // Say so rather than failing with a page of errors from inside the
        // standard library, which is what a raw-syscall bench does when it is
        // pointed at the wrong OS.
        const note = b.addSystemCommand(&.{
            "sh", "-c",
            "echo 'bench: skipped -- the benches are Linux-only (raw syscalls; gen and diskbench are io_uring).' >&2",
        });
        bench_step.dependOn(&note.step);
    } else for (benches) |name| {
        const exe = b.addExecutable(.{
            .name = name,
            .root_module = b.createModule(.{
                .root_source_file = b.path(b.fmt("bench/{s}.zig", .{name})),
                .target = target,
                .optimize = optimize,
            }),
        });
        const install = b.addInstallArtifact(exe, .{});
        bench_step.dependOn(&install.step);
    }

    // The example, as a module, so the end-to-end test can start the real
    // server rather than a copy of it.
    const echo_mod = b.createModule(.{
        .root_source_file = b.path("examples/echo.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "budgie", .module = budgie }},
    });

    const test_step = b.step("test", "Build and run the test programs");
    for (tests) |t| {
        const imports: []const std.Build.Module.Import = if (t.needs_echo)
            &.{ .{ .name = "budgie", .module = budgie }, .{ .name = "echo", .module = echo_mod } }
        else
            &.{.{ .name = "budgie", .module = budgie }};
        const exe = b.addExecutable(.{
            .name = t.name,
            .root_module = b.createModule(.{
                .root_source_file = b.path(b.fmt("tests/{s}.zig", .{t.name})),
                .target = target,
                .optimize = if (t.fast) optimize else test_optimize,
                .imports = imports,
            }),
        });

        const run = b.addRunArtifact(exe);
        run.addArgs(t.args);
        // `.inherit` rather than the default capture-and-check. These
        // programs report what they checked on stderr and that report is the
        // point of running them; captured, the build runner calls it
        // unexpected output and prints "failed command" beside a step that
        // passed. `.inherit` also fails the step on a non-zero exit on its
        // own -- so it replaces expectExitCode rather than needing it, and
        // the two together are a hard error -- and marks the step as having
        // side effects, so the suite re-runs instead of being cached away.
        run.stdio = .inherit;
        test_step.dependOn(&run.step);

        // Each test is also its own step, so a single one can be re-run with
        // different arguments: `zig build sim -- 7 256 600`.
        const solo = b.addRunArtifact(exe);
        solo.stdio = .inherit;
        if (b.args) |args| solo.addArgs(args) else solo.addArgs(t.args);
        b.step(t.name, b.fmt("Run {s}", .{t.name})).dependOn(&solo.step);
    }

    // Cross-compile everything without running it. The kernel is Linux-only,
    // so this is how a non-Linux host checks that a change still compiles.
    //
    // x86_64 and not the host arch: bench/diskbench.zig issues a raw `open`
    // syscall, which arm64 Linux does not have (it is openat-only), so a
    // native-linux check on an arm64 machine fails there and nowhere else.
    const check_query = b.option(
        []const u8,
        "check-target",
        "Target triple for the check step (default: x86_64-linux-gnu)",
    ) orelse "x86_64-linux-gnu";
    const check_target = b.resolveTargetQuery(std.Target.Query.parse(.{
        .arch_os_abi = check_query,
    }) catch @panic("invalid -Dcheck-target"));
    const check_step = b.step("check", "Typecheck everything for Linux (does not run)");
    const check_mod = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = check_target,
        .optimize = optimize,
    });
    for ([_][]const u8{ "app", "tests", "examples" }) |dir| {
        const names: []const []const u8 = if (std.mem.eql(u8, dir, "app"))
            &apps
        else if (std.mem.eql(u8, dir, "examples"))
            &.{"echo"}
        else
            &.{ "parser_test", "pipetest", "cancel_test", "feedcmp", "chunkfuzz", "sim" };
        for (names) |name| {
            const exe = b.addExecutable(.{
                .name = b.fmt("check-{s}", .{name}),
                .root_module = b.createModule(.{
                    .root_source_file = b.path(b.fmt("{s}/{s}.zig", .{ dir, name })),
                    .target = check_target,
                    .optimize = optimize,
                    .imports = &.{.{ .name = "budgie", .module = check_mod }},
                }),
            });
            check_step.dependOn(&exe.step);
        }
    }
    for (benches) |name| {
        const exe = b.addExecutable(.{
            .name = b.fmt("check-{s}", .{name}),
            .root_module = b.createModule(.{
                .root_source_file = b.path(b.fmt("bench/{s}.zig", .{name})),
                .target = check_target,
                .optimize = optimize,
            }),
        });
        check_step.dependOn(&exe.step);
    }
}

/// Whether an executable exists only on Linux.
fn isLinuxOnly(name: []const u8) bool {
    for (linux_only_apps) |n| {
        if (std.mem.eql(u8, n, name)) return true;
    }
    return false;
}
