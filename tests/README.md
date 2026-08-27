# tests

Ten programs, each with a `main` rather than `test` blocks: the fuzz and simulation drivers take a seed and an iteration count, and being able to re-run one by hand with a different seed is the point of them.

```sh
zig build test                  # all ten, in order; a non-zero exit fails
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
| `echo_test.zig`   | drives `examples/echo.zig` over real sockets: keep-alive, pipelined bursts, byte-at-a-time delivery, every split boundary, malformed and oversized input, hangups, half-close, 32 concurrent connections, and slot reuse |
| `server_test.zig` | drives `app/server.zig` the same way, for what the example leaves out: work units charged exactly, over-budget requests refused with 503, a storm of twenty of them, the control surface on the next port up and under load, and the counters agreeing afterwards |
| `starve_test.zig` | runs `app/server.zig` with a pool of 2 against 24 simultaneous connections, and holds it to answering every one of them with a 503 rather than closing in silence |
| `deadline_test.zig` | the idle deadline: a silent connection and a half-sent request are answered 408, an active one is never cut off, and the counters agree |
| `httpclient.zig`  | the blocking client the socket tests share, not a test itself |

Tests build at `ReleaseSafe`, not at `-Doptimize`: the suite states its invariants with `std.debug.assert`, which `ReleaseFast` compiles out. A green run in `ReleaseFast` would be checking almost nothing. `-Dtest-optimize=` overrides it; `sim` is the exception and builds at `-Doptimize`, because it checks a trace hash rather than assertions and runs six hours of virtual time.

On Linux the two socket tests run twice, once against `app/server.zig` over epoll and once against `app/server_uring2.zig` over io_uring, by pointing the module named `server` at the other file. The test sources are identical; only the binding changes.

`start` runs on the server's own thread rather than the caller's, and that placement is load-bearing. The io_uring build sets `IORING_SETUP_SINGLE_ISSUER`, which requires every submission to come from the task that created the ring. Creating it on one thread and submitting from another leaves the server accepting connections and answering none: 14 of 18 checks failed that way, and the four that passed were the ones that pass by receiving nothing.

`echo_test` starts the real example server on a thread, binds port 0 so runs cannot collide, and talks to it over the loopback interface. It exists because the rest of the suite never touched a socket, and two bugs lived in exactly that gap: a pipelined request the example kept in its buffer and never looked at again, and a clock call that passed a constant where it meant the time. Reintroducing either one now fails the build.
