# coopkernel

<img src="img/budgie-small.png" alt="coopkernel" width="180" align="right">

A cooperative scheduler for one thread, in Zig 0.16, built by applying seL4's
resource model to a userspace async runtime.

It is a research artifact, not a library. It runs, it is tested, and it has
real gaps listed at the bottom. What is worth your time here is the shape of
it, and `docs/STORY.md`, which records what went wrong on the way.

Two questions drove the design. What should a scheduler be allowed to know
about the work it schedules? And when a connection misbehaves, who is holding
the number that proves it?

---

## What it borrows from seL4

seL4 gives a thread a scheduling context: a budget, a period, and nothing
else. The kernel enforces it without knowing what the thread does. Separately,
memory comes from untyped capabilities that a process carves up itself, and
the kernel tracks the derivation tree without knowing what the memory is for.

Both ideas show up here.

`sched.zig` gives each task one opaque execution budget. It has no fd, no
syscall, no clock read, and no idea what a buffer, a byte, or a protocol phase
is. An earlier version had an `Account` enum inside the scheduler listing
`{accept, parse, work, write}`, which is a web server's phases compiled into a
CPU scheduler. Those labels now live in `accounts.zig`, where the application
defines them and the scheduler never sees them.

`quota.zig` is the derivation tree. A parent hands budget to children, a draw
deducts from every ancestor, and it is all or nothing. It is generic over the
unit, and refill is a policy field. It is deliberately not a scheduler
concept, because in seL4 it is not one either.

Where it stops borrowing: seL4 is a verified kernel with hardware protection,
and this is a cooperative loop in one userspace thread. A task that refuses to
yield is a problem no capability model solves. There is a watchdog for that,
and it reports rather than enforces.

---

## What the scheduler holds

Strict priority classes, with the highest non-empty one picked by `@ctz` on a
bitmask.

Three budget levels that answer different questions. A per-request `cap`, the
`budget` a task currently holds, and a supervisor tree where `topUp` deducts
from every ancestor. The third is the one that earns its keep, because it
makes "all connections together may spend X per period" expressible rather
than only per-connection limits.

A cleanup reserve the task body cannot touch, so an unwind is funded even when
the work that overran was not.

Generational cancel tokens. Cancellation is scheduler state, never a return
value, so no `catch {}` can swallow it.

An intrusive timer wheel with O(1) arm, disarm, and rearm, and no stale
entries.

Two currencies that never mix. Work units enforce and are deterministic.
Nanoseconds are observed and never read by control flow. That separation is
what lets the simulator run the real scheduler on a virtual clock and get the
same trace every time.

A `yieldCheck()` and a stall watchdog. If the running task has not passed a
yield point since the last tick, the supervisor gets a fault naming the task.
That turns "we are careful about yielding" from a convention into something
measured.

---

## Writing a server against it

`examples/echo.zig` is about 120 lines and names no operating system. Build it
on Linux and the reactor underneath is epoll. Build it on macOS and it is
kqueue. Nothing in the file changes.

The loop is the whole contract:

```zig
while (true) {
    while (s.popRunnable()) |t| step(t);   // run what is runnable
    _ = r.wait(&s, 1000);                  // block until an fd is ready
    s.expire(nowMs());                     // fire expired timers
}
```

A task is a state machine the scheduler hands control to. When it cannot make
progress, it parks itself against an fd and returns:

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

`r.read` belongs to the reactor rather than the caller, and that is not an
accident. A readiness reactor that hands out `watch` and lets the application
do the read never learns how many bytes anyone consumed, so fairness has to
live somewhere else and coordinate with it. Six attempts at a byte-fairness
policy failed that way before the read moved inside. The reactor owns the
read, so it owns the bytes, so it owns both the accounting and the arming, and
a pause cannot race its own resume.

Run it with `zig build echo`, then `curl localhost:8080`.

---

## Reactors

Five backends have been written against the same contract: poll, epoll,
io_uring readiness, io_uring completion, and kqueue. `sched.zig` was unchanged
across every one of them.

The kqueue port did not need a fifth copy of the reactor. Of `reactor.zig`'s
400 lines, about 15 were ever epoll-specific, and those are now
`backend_epoll.zig` and `backend_kqueue.zig`. The client table, the byte
accounting, and the round policy stay shared. The semantics line up because
`EV_ONESHOT` and `EPOLLONESHOT` mean the same thing. An event fires, the
registration is gone, and the task stays parked until something rearms it.

Be careful with the word portable here. The readiness backends really are
interchangeable, and `examples/echo.zig` is the proof. The io_uring completion
build is not. It needs a different application shape, which is why
`app/server_uring2.zig` is a separate file from `app/server.zig` rather than
the same file with a flag.

---

## Determinism

`tests/sim.zig` imports the unmodified scheduler and runs it on a virtual
clock. The same seed gives a byte-identical trace hash over a million
dispatches, and six hours of virtual time takes seconds of real time.

The hash also matches across macOS on arm64 and Linux on x86_64, so the
scheduler is deterministic across architecture and operating system, not only
across runs on one machine.

---

## Layout

```
src/
  sched.zig            the scheduler. The only file with no I/O in it.
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

That is disk, not memory. `iobuf.store` is a 135 MB static pool declared
`undefined`. ReleaseFast leaves it uninitialised so it sits in `.bss` and
occupies no bytes in the file, while Debug writes `0xAA` over it, which moves
it into `.data` and into the binary. `.data` accounts for 181 MB of the Debug
file; most of the rest is padding, since the loadable segments do not start
until roughly 252 MB in.

Neither number is what the process costs to run. Measured on Linux:

| | file | virtual | resident |
|---|---|---|---|
| Debug | 498.2 MB | 185.2 MB | 3.8 MB |
| ReleaseFast | 4.7 MB | 137.5 MB | 2.4 MB |

The pool is reserved address space in both. Only the pages actually touched
are ever faulted in, and at the default of 64 buffers that is about a
megabyte, so resident memory stays under 4 MB either way. The Debug binary is
also not sparse, though it gzips to 3.1 MB if you need to move one.

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

- One carrier, no threads. Multi-core is designed, shard rather than steal,
  and not built.
- Tasks are hand-rolled state machines rather than fibers. There is no
  `perform` or `around`.
- The io_uring completion build does not scale with queue depth, because a
  provided buffer ring gives global backpressure rather than per-connection
  flow control. See APIGUIDE.
- `iobuf` has no "the kernel owns this" state, which an IOCP port would need
  first.
- The benches still speak raw Linux syscalls and have not been ported, so
  generating load on macOS needs an outside tool.
- `bench/diskbench.zig` uses a raw `open` syscall and will not compile for
  arm64 Linux, which is openat only.

Numbers from the original measurements are in `results/` and discussed in
`docs/STORY.md`. Treat them as a record of one session on one shared vCPU with
the load generator running on the same core, not as a claim about this design.

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
kernel succeeds on the first call and never sees a non-zero reserved field.
When the fallback runs, the server says so in its stats. 6.8.0-134 is the last
version known to be unaffected.

---

## Reading order

1. `docs/STORY.md`. What happened and what it taught. Start here.
2. `examples/echo.zig`. The smallest thing that uses the kernel.
3. `docs/APIGUIDE.md`. How the pieces fit, and every knob.
4. `src/sched.zig`. The core, and the only file with no I/O in it.
