# tests

Six programs, each with a `main` rather than `test` blocks: the fuzz and simulation drivers take a seed and an iteration count, and being able to re-run one by hand with a different seed is the point of them.

```sh
zig build test                  # all six, in order; a non-zero exit fails
zig build sim        -- 42 64 600
zig build chunkfuzz  -- 200000 0xBADF00D
```

They import the kernel as `@import("budgie")`. There is nothing to link or copy first -- an earlier version of this directory kept a farm of symlinks back to `../src`, which `build.zig` now replaces.

| | what it holds down |
|---|---|
| `parser_test.zig` | split-invariance, pipelining, malformed input, fuzz |
| `pipetest.zig`    | a pipelined burst walked through the parser |
| `chunkfuzz.zig`   | byte-stream invariant under adversarial chunk boundaries -- the class of bug the scheduler DST cannot see and real I/O finds only by luck |
| `cancel_test.zig` | cancellation semantics |
| `feedcmp.zig`     | old vs new parser `feed()`, for the record |
| `sim.zig`         | deterministic simulation of the real scheduler on a virtual clock |

Tests build at `ReleaseSafe`, not at `-Doptimize`: the suite states its invariants with `std.debug.assert`, which `ReleaseFast` compiles out. A green run in `ReleaseFast` would be checking almost nothing. `-Dtest-optimize=` overrides it; `sim` is the exception and builds at `-Doptimize`, because it checks a trace hash rather than assertions and runs six hours of virtual time.

All but `sim` run on a non-Linux host unchanged -- `sched`, `quota` and `http` contain no I/O, and Zig never analyses the reactors if nothing references them. `sim` needs Linux only for the stopwatch it reports its own runtime with.
