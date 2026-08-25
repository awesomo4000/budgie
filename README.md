# coopkernel

A single-threaded cooperative kernel in userspace, written in Zig 0.16, built
in one session as an exploration of seL4-style scheduling ideas applied to a
hosted async runtime.

It is a scheduler with budgets, priorities, supervisors, deadlines and
cancellation; a swappable I/O reactor (poll / epoll / io_uring readiness /
io_uring completion); a sans-I/O HTTP parser; a deterministic simulator that
drives the real scheduler on a virtual clock; and a set of load generators and
benchmarks.

It is a **research artifact**, not a library. It has known gaps (see
"Status"). The value is in the design and in `docs/STORY.md`, which records
what went wrong and why.

---

## Layout

```
src/
  sched.zig            scheduler: priority classes, timer wheel, generational
                       cancellation, ONE opaque execution budget per task.
                       No fd, no syscall, no clock read, and no idea what a
                       buffer, a byte or a protocol phase is.
  quota.zig            conservation trees over a resource. Parent/child, draw
                       deducts from every ancestor, all-or-nothing. Generic
                       over the unit; refill is a policy field (periodic /
                       on_return / never). NOT a scheduler concept -- seL4
                       derives scheduling contexts and untypeds from the same
                       capability tree and the kernel does not know what the
                       memory is for.
  accounts.zig         per-phase telemetry with APPLICATION-defined labels.
                       This was an `Account` enum inside the scheduler reading
                       {accept, parse, work, write, ...} -- HTTP server phases
                       in a CPU scheduler.
  reactor.zig          readiness reactor: client table, byte accounting and
                       round policy. Platform-free; the wakeup primitive is a
                       backend.
  backend_epoll.zig    the epoll half of it (Linux)
  backend_kqueue.zig   the kqueue half of it (macOS, BSD)
  interest.zig         what a task waits for; shared by the two above
  sys.zig              sockets, tick timer, process introspection -- the
                       platform seam for the application
  reactor_uring.zig    io_uring reactor, IORING_OP_POLL_ADD (readiness)
  reactor_uring2.zig   io_uring reactor, multishot recv + buffer ring
                       (completion). Registered files and buffers.
  http.zig             sans-I/O HTTP request parser: bytes in, events out.
                       No fd, no allocator, no suspension point.
  iobuf.zig            generational pool of I/O buffers
  clock.zig            real or virtual time, injected
  drr.zig              deficit round robin over byte flows; off by default
  root.zig             module root, re-exporting the ten files above

app/
  server.zig           application driving sched + reactor.zig (epoll)
  server_uring2.zig    application driving sched + reactor_uring2 (completion)

tests/
  sim.zig              deterministic simulation of the REAL scheduler
  parser_test.zig      split-invariance, pipelining, malformed input, fuzz
  pipetest.zig         pipelined burst walked through the parser
  chunkfuzz.zig        byte-stream invariant under adversarial chunk
                       boundaries -- the class of bug the scheduler DST
                       cannot see and real I/O finds only by luck
  cancel_test.zig      cancellation semantics
  feedcmp.zig          old vs new parser feed(), for the record

bench/
  gen.zig              io_uring load generator, correct HTTP framing
  bench.zig            original poll load generator (kept; see STORY.md)
  bench2.zig           pipelining poll load generator (kept; see STORY.md)
  hold.zig             opens N idle keep-alive connections (memory tests)
  ctrl.zig             control-surface latency probe
  diskbench.zig        O_DIRECT random reads, pread vs io_uring queue depth
  iobench.zig          page-cached reads (shows what NOT to measure)
  sysc.zig             raw syscall cost
  *.sh                 sweep drivers

build.zig              module, executables, and the test/bench/check steps
stages/                every intermediate version, srv .. srv14, in order
results/               CSVs and plots from the measurements
docs/                  architecture diagram, API guide, the story
```

`src/` is the kernel and nothing else: ten files with no `main` between them,
exported as the `coopkernel` module. `app/`, `tests/` and `bench/` are
consumers of it and reach it as `@import("coopkernel").sched`. Inside `src/`
the files still import each other by path, so each one's dependencies stay
visible at the top of the file.

Every program under `bench/` imports only `std`. They are standalone by
design, so a load generator can be built and copied to a separate load box
without carrying the kernel with it.

`stages/` is the history. Each directory is a working checkpoint; the
progression is described in `docs/STORY.md`. It is kept for the record and is
deliberately outside the build -- those files are snapshots, not sources.

---

## Build and run

Requires Zig 0.16.0.

| | Linux | macOS |
|---|---|---|
| `zig build test` | 6/6 pass | 6/6 pass |
| `server` (readiness reactor) | epoll | kqueue |
| `server_uring2` (completion) | io_uring | not available |
| `zig build bench` | yes | not ported |

Both columns are measured, not assumed. The Linux side was built and run
natively on Ubuntu 6.8.0 / x86_64 -- `zig build -Drelease`, `zig build test`,
`zig build bench` and `zig build check` all pass there, and the bench step
builds all eight generators.

It also runs cross-compiled: binaries built on an arm64 Mac execute on that
Linux box with nothing installed, because the Linux build links no libc at
all. Nothing calls `linkLibC`, the ELF is static, and it carries zero libc
symbols. macOS cannot say the same and never will -- there is no stable
syscall ABI there, so every binary links `libSystem`.

`sim` produces a **byte-identical trace hash on both** -- `663f011e3a5bb05c`
for the default seed on macOS/arm64 and Linux/x86_64 alike. The scheduler is
deterministic across architecture and operating system, not merely across
runs on one machine.

```sh
zig build -Drelease                        # servers -> zig-out/bin
zig build test                             # the six test programs
zig build check                            # typecheck for Linux without running
```

`-Drelease` and not `-Doptimize=ReleaseFast`: 0.16's `standardOptimizeOption`
exposes the former and rejects the latter outright. **Build with it.** A Debug
server is ~500 MB, because `iobuf.store` is a 141 MB `undefined` static pool
and Debug writes `0xAA` over it, which moves the whole thing out of `.bss` and
into the binary. ReleaseFast leaves it uninitialised and the same binary is
1.3 MB.

The io_uring build is Linux-only and there is nothing to port it to: kqueue is
a readiness interface, and that build exists precisely to exercise completion
with a kernel-owned buffer ring. `build.zig` drops it from a non-Linux build
rather than failing, and naming `reactor_uring2` on such a target is a one-line
error instead of a wall of io_uring internals.

`zig build check` cross-compiles every executable for `x86_64-linux-gnu`
without running anything, so a change made on a Mac is verified against the
Linux path too. Pass `-Dcheck-target=` to aim it elsewhere -- note that
`bench/diskbench.zig` issues a raw `open` syscall and so does not compile for
arm64 Linux, which is openat-only.

Run a server:

```sh
./zig-out/bin/server 8080 30            # port, seconds to run
./zig-out/bin/server 8080 30 io_bufs=32 quantum_units=250 conn_quota=50000

zig build server -- 8080 30             # or straight from the build system
```

Every knob is `key=value` on the command line; see `docs/APIGUIDE.md`.

Tests:

```sh
zig build test                  # builds and runs all six; non-zero exit fails
zig build sim -- 42 64 600      # or one at a time, with your own arguments
```

The test programs are ordinary executables with a `main`, not `test` blocks,
because the fuzz and simulation drivers take a seed and an iteration count and
re-running one by hand with a different seed is the point of them. They build
at `ReleaseSafe` rather than at `-Doptimize`, since the suite checks its
invariants with `std.debug.assert` and `ReleaseFast` compiles those out; use
`-Dtest-optimize=` to override.

Five of the six run on a non-Linux host as they are -- `sched`, `quota` and
`http` genuinely contain no I/O, and Zig never analyses the reactors if
nothing references them. Only `sim` needs Linux, and only for the stopwatch it
uses to report how long the run took.

Load:

```sh
zig build bench                 # all eight generators and probes -> zig-out/bin
./zig-out/bin/gen <port> <conns> <secs> <depth> <units>
# or just use wrk:
wrk -t1 -c256 -d5s --latency http://127.0.0.1:8080/work/0
```

The toy protocol is `GET /work/<N>` where `N` is how many units of CPU work
the request asks for. That makes request cost a parameter, which is what the
budget experiments need.

---

## What it does

**Scheduler (`sched.zig`)**

- Strict priority classes, highest non-empty selected with `@ctz` on a bitmask.
- Three distinct budget levels: a per-request `cap`, the `budget` grant a task
  currently holds, and a supervisor tree where `topUp` deducts from every
  ancestor. "All connections together may spend X per period" is expressible,
  not just per-connection limits.
- A cleanup reserve the body can never touch, so an unwind is funded even when
  the work that overran was not.
- Generational cancel tokens. Cancellation is scheduler *state*, never a return
  value, so there is no `catch {}` that can swallow it.
- Intrusive timer wheel: O(1) arm / disarm / rearm, no stale entries.
- Two currencies: work units enforce and are deterministic; nanoseconds are
  observed and never read by control flow.
- `drr.zig`: deficit round robin over byte flows, enforced by pausing a
  connection's recv. Byte volume is the currency the *peer* controls, so it is
  the only account that can bound a greedy client. Works on both backends and
  is OFF BY DEFAULT on both, because the metric it improves turned out not to
  measure service quality -- see APIGUIDE, "What unfair actually turned out to
  mean". The real tail effect was `bounded_drain`'s resolution, fixed by a 2ms
  default tick at no throughput cost.
- A cooperative `yieldCheck()` plus a stall watchdog: the tick stops enforcing
  and starts *reporting*. If the running task has not passed a yield point
  since the last tick, the supervisor gets a fault naming the task. Turns "we
  are careful about yielding" from a convention into a measured property.

**Reactors** — same four-function interface, five implementations: poll,
epoll, io_uring readiness, io_uring completion, and kqueue. `sched.zig` was
byte-identical across all of them.

The kqueue port did not need a fifth copy of the reactor. Only about fifteen
of `reactor.zig`'s four hundred lines were ever epoll-specific -- arm, disarm,
wait, and the read -- and those are now `backend_epoll.zig` and
`backend_kqueue.zig`. The client table, the byte accounting and the round
policy are shared, which is the part worth not having two of. The semantics
line up because `EV_ONESHOT` is `EPOLLONESHOT`: an event fires, the
registration is gone, and the task stays parked until something rearms it.

**Determinism** — `sim.zig` imports the unmodified scheduler and runs it on a
virtual clock. Same seed produces a byte-identical trace hash over a million
dispatches; six hours of virtual time run in 35 seconds of wall clock.

---

## Measurements

All on a **single shared vCPU** with the load generator on the same core, so
absolute numbers are low and only comparable within a run. See the caveat in
`docs/STORY.md` — the box drifted ~30% over the session.

| | epoll | io_uring (completion) |
|---|---|---|
| wrk, 256 conns | 91-99k req/s | 132-143k req/s |
| p50 / p99 | 2.15-2.33 / 3.87-4.28 ms | 1.34-1.43 / 2.46-2.74 ms |
| non-2xx | 0 | 0 |
| idle RSS | 2.1 MB | 2.7 MB |
| 4096 idle conns | +4 KB | +228 KB |
| best `io_bufs` | ~1 per connection | ~64, far below conn count |

Numbers are from the end of the session and are ~25% below ones taken earlier
the same day on unchanged code -- the VM drifted. Only compare figures taken
within the same few minutes; every A/B in this repo was interleaved for that
reason.

**`io_bufs` has opposite optima on the two backends, for a structural reason.**
The readiness build acquires a buffer *before* reading, so every readable
connection holds one simultaneously and it needs roughly one per connection:
at 256 connections, `io_bufs=64` produced 95,522 `no_buffer` 503s and 16k
req/s. The completion build acquires only once bytes have arrived and
processes completions serially, so its high-water mark was 64 against 256 --
and a small pool is also an 8x latency win there. Defaults are now set per
build.

**The buffer pool is a concurrency limiter.** Shrinking `io_bufs` from 1024 to
32 took p50 from 1.40 ms to 168 us with throughput flat and *zero* requests
shed. epoll cannot do this: its buffers are effectively per-connection, so the
same sweep starves it to 7k req/s.

**Disk is where io_uring actually wins.** O_DIRECT 4K random reads:

```
pread, 1 in flight            22,038 IOPS    1.00x
io_uring, 1 in flight         26,217 IOPS    1.19x
io_uring, 64 in flight       341,864 IOPS   15.51x
io_uring, 128 in flight      367,778 IOPS   16.69x
```

The entire advantage is queue depth. On the network side the two reactors are
within noise of each other; on storage it is an order of magnitude.

---

## Status

Working: scheduler, all four reactors, supervisors, cancellation, timer wheel,
sans-I/O parser, deterministic simulation, buffer pool, control surface.

Known gaps:

- Single carrier. No threads. Multi-core is designed (shard, don't steal) but
  not built.
- The benches still speak raw Linux syscalls and have not been ported, so load
  generation on macOS needs an external tool such as `wrk`.
- **Ubuntu 6.8.0-136 has an inverted check in `io_register_pbuf_ring`.** It
  returns `EINVAL` when the `io_uring_buf_reg` reserved fields are correctly
  zeroed, and succeeds when they are not -- so every spec-compliant io_uring
  program, liburing and Zig's standard library included, is unable to
  register a provided buffer ring on that kernel. That takes `server_uring2`
  and `gen` with it, since both depend on one.

  This was traced rather than guessed. A `kretprobe` shows
  `io_register_pbuf_ring` entered and returning `-22` before `io_pin_pages`
  is ever reached, so it fails in the early validation block. Disassembling
  the function from `/proc/kcore` shows `memchr_inv(&resv, 0, 24)` followed
  by a `je` into the `EINVAL` path -- taken precisely when the reserved
  fields are all zero. Directly: `resv` zeroed gives `EINVAL`, `resv[0]=1`
  gives `SUCCESS`, and the result does not depend on ordering or buffer group
  id. Everything around it is healthy -- `io_uring_setup` reports every
  feature bit, `REGISTER_FILES`/`REGISTER_BUFFERS`/`REGISTER_PROBE`/
  `REGISTER_IOWQ_MAX_WORKERS` all succeed, and `REGISTER_PROBE` reports
  `PROVIDE_BUFFERS`, `RECV` and `POLL_ADD` supported up to `last_op=54`.

  The completion reactor itself is fine. Patched past the bad check locally
  -- not committed, since writing junk into a reserved field is only correct
  against a kernel this broken -- `server_uring2` serves 1,102,349 requests
  at **91.9k req/s** against epoll's 67-81k on the same two cores, with
  multishot recv rearming cleanly, `enobufs=0`, and buffer acquires equal to
  releases. So the io_uring advantage the table below claims does reproduce;
  it is the kernel, not the reactor, that is broken here.
- Tasks are hand-rolled state machines, not fibers. No `perform` / `around`.
- io_uring completion build no longer *fails* at depth, but does not scale with
  it: 44-53k req/s from depth 32 to 256, flat, while epoll climbs 66k -> 198k.
  No connection errors at any depth. Cause is that a provided buffer ring gives
  GLOBAL backpressure, not per-connection flow control -- see APIGUIDE.
- `iobuf` has no "kernel owns this" state. Required before an IOCP port.
- Numbers need re-measuring on a machine that is not also running the client.

---

## Reading order

1. `docs/STORY.md` — what happened, what went wrong, what it taught. Start here.
2. `docs/APIGUIDE.md` — how the pieces fit and how to use them.
3. `src/sched.zig` — the core, and the only file with no I/O in it.
