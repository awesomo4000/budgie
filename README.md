# coopkernel

<img src="img/budgie-small.png" alt="coopkernel" width="180" align="right">

A cooperative scheduler for one thread, in Zig 0.16, built by applying seL4's
resource model to a userspace async runtime.

This is a research artifact. It runs, it is tested, and the gaps are listed at
the bottom. The interesting part is the shape of it, and `docs/STORY.md`,
which records what went wrong on the way.

Two questions drove the design. What should a scheduler be allowed to know
about the work it schedules? And when a connection misbehaves, who is holding
the number that proves it?

---

## What it borrows from seL4

seL4 gives a thread a scheduling context: a budget and a period. The kernel
enforces the thread from those two numbers alone. Memory comes from untyped
capabilities that a process carves up itself, and the kernel tracks the
derivation tree while the meaning of the memory stays with the process.

Both ideas show up here.

`sched.zig` gives each task one opaque execution budget. Its whole vocabulary
is task ids, priorities, budgets, deadlines, and a cancel bit. An earlier
version had an `Account` enum inside the scheduler listing `{accept, parse,
work, write}`, which is a web server's phases compiled into a CPU scheduler.
Those labels now live in `accounts.zig`, where the application defines them
and reads them back.

`quota.zig` is the derivation tree. A parent hands budget to children, a draw
deducts from every ancestor, and it is all or nothing. It is generic over the
unit, and refill is a policy field. It sits beside the scheduler, the way seL4
keeps capability derivation separate from scheduling.

Where the borrowing stops: seL4 is a verified kernel with hardware protection,
and this is a cooperative loop in one userspace thread. A task that holds the
CPU is a problem capabilities leave open. The watchdog covers it by reporting,
described below.

---

## What the scheduler holds

Strict priority classes, with the highest non-empty one picked by `@ctz` on a
bitmask.

Three budget levels that answer different questions. A per-request `cap`, the
`budget` a task currently holds, and a supervisor tree where `topUp` deducts
from every ancestor. The third one earns its keep, because it makes "all
connections together may spend X per period" expressible.

A cleanup reserve that only the unwind path can spend, so releasing a
connection stays funded after the work that overran it.

Generational cancel tokens. Cancellation lives in scheduler state, so it
survives whatever error handling the task does on the way out.

An intrusive timer wheel with O(1) arm, disarm, and rearm.

Two currencies with separate jobs. Work units enforce, and they are
deterministic. Nanoseconds are recorded for telemetry. Keeping the clock out
of control flow is what lets the simulator run the real scheduler on a virtual
clock and get the same trace every time.

A `yieldCheck()` and a stall watchdog. If the running task has passed no yield
point since the last tick, the supervisor gets a fault naming the task. That
turns "we are careful about yielding" from a convention into something
measured.

---

## Writing a server against it

`examples/echo.zig` is about 120 lines of code whose only imports are the
kernel and `std`. Build it on Linux and the reactor underneath is epoll. Build
it on macOS and it is kqueue. The same file compiles for both.

The loop is the whole contract:

```zig
while (true) {
    while (s.popRunnable()) |t| step(t);   // run what is runnable
    _ = r.wait(&s, 1000);                  // block until an fd is ready
    s.expire(nowMs());                     // fire expired timers
}
```

A task is a state machine the scheduler hands control to. It runs until it
would block, parks itself against an fd, and returns:

```zig
fn step(t: TaskId) void {
    const c = &conns[t];
    const n = r.read(t, c.fd, c.in[c.in_len..]);
    if (n < 0) return r.watch(t, c.fd, .read);   // would block, park again
    if (n == 0) return finish(t, c);             // peer hung up
    c.in_len += @intCast(n);

    const used = c.parser.feed(c.in[0..c.in_len]);
    consume(c, used);                            // keep any pipelined remainder

    switch (c.parser.poll()) {
        .need_input => r.watch(t, c.fd, .read),
        .protocol_error => finish(t, c),
        .request => |req| respond(t, c, req),
    }
}
```

Notice that `r.read` is the reactor's call. That placement is the answer to
the second question at the top. The reactor does the read, so it sees the byte
count, so it owns the accounting and the arming together, and a pause happens
in the same code that will resume it. Six attempts at a byte-fairness policy
came apart before the read moved inside, each of them splitting the count from
the decision that used it.

Run it with `zig build echo`, then `curl localhost:8080`.

---

## Reactors

Five backends have been written against the same contract: poll, epoll,
io_uring readiness, io_uring completion, and kqueue. `sched.zig` stayed
byte-identical through all of them.

The kqueue port reused the reactor whole. Of `reactor.zig`'s 400 lines, about
15 were epoll-specific, and those 15 are now `backend_epoll.zig` and
`backend_kqueue.zig`. The client table, the byte accounting, and the round
policy stay shared. The semantics line up because `EV_ONESHOT` and
`EPOLLONESHOT` mean the same thing. An event fires, the registration is gone,
and the task stays parked until something rearms it.

That interchangeability covers the readiness backends, and `examples/echo.zig`
is the proof. The io_uring completion build asks for a different application
shape, since the kernel fills buffers and hands back completions, so
`app/server_uring2.zig` is its own file.

---

## Determinism

`tests/sim.zig` imports the unmodified scheduler and runs it on a virtual
clock. The same seed gives a byte-identical trace hash over a million
dispatches, and six hours of virtual time takes seconds of real time.

The hash also matches across macOS on arm64 and Linux on x86_64, so the
determinism holds across architecture and operating system as well as across
runs.

---

## Layout

```
src/
  sched.zig            the scheduler. Pure logic, I/O free.
  quota.zig            conservation trees over a resource
  accounts.zig         per-phase telemetry, labels defined by the application
  reactor.zig          readiness reactor: client table, byte accounting, rounds
  backend_epoll.zig    the epoll half of it (Linux)
  backend_kqueue.zig   the kqueue half of it (macOS)
  interest.zig         what a task waits for
  reactor_uring.zig    io_uring, IORING_OP_POLL_ADD (readiness)
  reactor_uring2.zig   io_uring, multishot recv and a buffer ring (completion)
  uring_bufring.zig    buffer ring registration, around a broken kernel
  http.zig             sans-I/O request parser: bytes in, events out
  iobuf.zig            generational pool of I/O buffers
  drr.zig              deficit round robin over byte flows, off by default
  clock.zig            real or virtual time, injected
  sys.zig              sockets and process introspection, per platform
  root.zig             module root

app/         the two servers, epoll/kqueue and io_uring completion
examples/    echo.zig, the small backend-independent server
tests/       parser, cancellation, pipelining, chunk fuzzing, the simulator
bench/       load generators and microbenchmarks (Linux only)
docs/        APIGUIDE.md, STORY.md, architecture diagram
results/     CSVs and plots from the original measurements
stages/      srv..srv14, every checkpoint, kept as history
```

---

## Build and run

Requires Zig 0.16.0.

```sh
zig build -Drelease        # servers and the example into zig-out/bin
zig build echo             # run the example server
zig build test             # six test programs
zig build check            # typecheck everything for Linux without running
```

Use `-Drelease`. Zig 0.16 exposes that flag and rejects `-Doptimize`. It also
matters more than usual here, because a Debug build of the server is a 498 MB
file against 4.7 MB for ReleaseFast.

Those are file sizes. `iobuf.store` is a 135 MB static pool declared
`undefined`. ReleaseFast leaves it uninitialised, so it lands in `.bss` and
costs zero bytes on disk. Debug writes `0xAA` over it, which moves it into
`.data` and into the file. `.data` accounts for 181 MB of the Debug binary,
and the linker adds another 247 MB of padding between segments, which is where
the rest goes.

Memory is a separate question. Measured on Linux:

| | file | virtual | resident |
|---|---|---|---|
| Debug | 498.2 MB | 185.2 MB | 3.8 MB |
| ReleaseFast | 4.7 MB | 137.5 MB | 2.4 MB |

The pool is reserved address space in both builds, and the kernel faults in
only the pages actually touched, which at the default of 64 buffers is about a
megabyte. Resident memory stays under 4 MB either way. The Debug binary
occupies its full 498 MB on disk and gzips to 3.1 MB.

|  | Linux | macOS |
|---|---|---|
| `zig build test` | 6/6 | 6/6 |
| `examples/echo.zig` | epoll | kqueue |
| `app/server.zig` | epoll | kqueue |
| `app/server_uring2.zig` | io_uring | not available |
| `zig build bench` | yes | not ported |

The Linux build links no libc. Nothing calls `linkLibC`, the ELF is static,
and it carries no libc symbols, so a binary cross-compiled on a Mac runs on a
Linux box with nothing installed. macOS is the opposite and always will be,
because there is no stable syscall ABI there.

`zig build check` cross-compiles every executable for `x86_64-linux-gnu`
without running it, which is how a change made on a Mac gets checked against
the Linux path.

---

## Status

Working: the scheduler, all five reactors, supervisors, cancellation, the
timer wheel, the parser, the simulator, the buffer pool, and the control
surface.

Gaps, in the order I would fix them:

- One carrier, single threaded. Multi-core is designed as sharding, and still
  on paper.
- Tasks are hand-rolled state machines. Fibers, `perform` and `around` are
  still open.
- The io_uring completion build stays flat as queue depth rises. A provided
  buffer ring gives global backpressure, so per-connection flow control has to
  come from somewhere else. See APIGUIDE.
- `iobuf` needs a "the kernel owns this" state before an IOCP port is
  possible.
- The benches speak raw Linux syscalls, so generating load on macOS needs an
  outside tool such as wrk.
- `bench/diskbench.zig` uses a raw `open` syscall, so it compiles for x86_64
  Linux only. arm64 is openat only.

Numbers from the original measurements are in `results/` and discussed in
`docs/STORY.md`. Read them as a record of one session on one shared vCPU with
the load generator running on the same core.

### A kernel bug worth knowing about

Ubuntu 6.8.0-136 and -137 return `EINVAL` from `io_register_pbuf_ring` when
the `io_uring_buf_reg` reserved fields are correctly zeroed, and succeed when
they are not. The check is inverted, so every caller that follows the spec,
liburing and Zig's standard library included, cannot register a provided
buffer ring at all. That takes `app/server_uring2.zig` and `bench/gen.zig`
with it.

It is [Launchpad #2162843](https://bugs.launchpad.net/ubuntu/+source/linux/+bug/2162843),
still open. The cause is a mangled backport of upstream commit `1724849`,
which rewrote `if (reg.resv[0] || reg.resv[1] || reg.resv[2])` as
`if (!mem_is_zero(reg.resv, sizeof(reg.resv)))`. `mem_is_zero` is
`memchr_inv(s, 0, n) == NULL`, so upstream compiles to a `jne` into the error
path. The shipped kernel has a `je`. The negation was lost on the way in, and
disassembling the function out of `/proc/kcore` is what confirmed it.

`src/uring_bufring.zig` works around it. It registers the ring correctly
first and falls back to the inverted form only on `EINVAL`, so a correct
kernel succeeds on the first call and stays on the spec-correct path. When the
fallback runs, the server says so in its stats. 6.8.0-134 is the last version
known to be unaffected.

---

## Reading order

1. `docs/STORY.md`. What happened and what it taught. Start here.
2. `examples/echo.zig`. The smallest thing that uses the kernel.
3. `docs/APIGUIDE.md`. How the pieces fit, and every knob.
4. `src/sched.zig`. The core, and the one file that touches no I/O.
