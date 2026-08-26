# budgie

<img src="img/budgie-small.png" alt="budgie" width="180" align="right">

An experiment: build an async runtime by copying the architecture of a microkernel instead of the architecture of a promise library.

## The observation

Stare at an async runtime long enough and it starts to look like an operating system. An asynchronous runtime divides a program into small pieces so that they can run in any order, which allows concurrent execution of those pieces and gives a program the ability to multiplex many operations at once. That freedom to run pieces as separate tasks is also what allows parallelism, by executing the pieces on different threads.

Both multiplex reads and writes over many descriptors at once, so that no descriptor gets a thread of its own to block. Both run computation over whatever arrives, in pieces, without knowing in advance which piece comes next. Both keep a pile of independent handlers alive at once and switch between them. Both have background work that should happen when nothing more urgent wants the machine. And both, at the bottom, answer the same question a few million times a second: given everything that could run right now, what runs next?

The piece that decides what should run is a scheduler. In many async runtimes we do not call it that. We call it an executor, a reactor, a task queue, or a disruptor, and we give it an API shaped like function calls so it feels like ordinary code. The kernel underneath is real either way. It is simply undocumented, and it usually has no notion of a budget, so a task that runs long stops every other task on that thread, and the runtime has no mechanism to detect it or account for it.

## The idea

Operating systems people have been at this for fifty years, and one branch of that work went somewhere unusual. seL4 is a microkernel small enough that its implementation has been proved to match its specification. It flies drones and sits inside medical devices. It is about ten thousand lines.

The thought that started this project was to open the hood of one of those, lift the engine out, and drop it into an ordinary userspace process as an async runtime.

The proofs do not come along. They belong to seL4's specification and do not survive being lifted out of it. What does transplant is the shape, and seL4's shape is unusual in two ways worth stealing.

A thread there gets a *scheduling context*: a budget and a period. The kernel enforces the thread from those two numbers and knows nothing else about it. Memory comes from untyped capabilities the process carves up itself, and the kernel tracks the derivation tree while the meaning of the memory stays with whoever asked for it. In both cases the kernel holds an accounting structure and refuses to hold a semantic one.

Apply that to a runtime and you get a scheduler whose entire vocabulary is task ids, priorities, budgets, deadlines, and a cancel bit. It holds no file descriptors, performs no syscalls, and cannot read the clock. It has no representation of a request, a connection, or a protocol.

## Whether that is a good idea

I do not know yet. That is what building it was for.

The honest description of what is here is a cooperative scheduler with budgets and cancellation, deliberately kept ignorant of I/O, plus a reactor that knows about I/O and nothing about scheduling policy. It runs on one thread, and nothing about it has been verified. Calling it a kernel is a claim about how the pieces are arranged and about nothing else.

What I wanted was a read on how it *feels* to write programs this way, next to the alternatives:

- **async/await** colours your functions and hands back cancellation as a value, so any `catch` along the path can swallow it.
- **Goroutines and channels** give you cheap concurrency and no answer at all to "this handler is eating the machine".
- **BEAM processes** are the closest relative and got there decades earlier: isolation, preemption, supervision trees. Erlang solved the runaway task by preempting it. This meters instead, and stays cooperative, which is a weaker guarantee bought at a lower price.
- **Zig's `std.Io`** threads an explicit `Io` through every caller, a different and defensible answer to the same question about who owns effects.

Against those, what differs here is that a budget is a number the scheduler holds, so an overrun is something the runtime measures rather than something the application is trusted to avoid. Whether that is worth the extra bookkeeping is the open question. `docs/LESSONS.md` is the running record.

## What it looks like to use

`examples/echo.zig` is about 120 lines whose only imports are this kernel and `std`. The loop is the whole contract:

```zig
while (true) {
    while (s.popRunnable()) |t| step(t);   // run what is runnable
    _ = r.wait(&s, 1000);                  // block until an fd is ready
    s.expire(nowMs());                     // fire expired timers
}
```

A task is a state machine the scheduler hands control to. It runs until it would block, parks itself against a descriptor, and returns:

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

Two things in there are load-bearing and easy to miss.

`r.read` is the *reactor's* call, not the task's. A readiness reactor that hands out `watch` and lets the application do the read never learns how many bytes anyone consumed, so any fairness policy has to live somewhere else and coordinate with it. Six attempts came apart that way before the read moved inside. Now the reactor sees the byte count, so it owns the accounting and the arming together, and a pause happens in the same code that will resume it.

The second is that the file names no operating system. Build it on Linux and the reactor underneath is epoll. Build it on macOS and it is kqueue. The same file compiles for both. `zig build echo`, then `curl localhost:8080`.

## What the scheduler holds

The scheduler keeps strict priority classes, and it selects the highest non-empty one with `@ctz` on a bitmask.

It tracks three budget levels, which answer different questions. A per-request `cap` bounds one unit of work. The `budget` is what a task currently holds. Above both sits a supervisor tree, where `topUp` deducts from every ancestor, and that third level is the interesting one because it makes "all connections together may spend X per period" expressible.

Each task also carries a cleanup reserve that only the unwind path can spend, so releasing a connection stays funded even after the work that overran it.

Cancel tokens are generational, and cancellation lives in scheduler state rather than travelling as a return value, so it survives whatever error handling the task does on the way out.

Timers live in an intrusive wheel that arms, disarms, and rearms in constant time.

The scheduler counts two currencies and keeps them apart. Work units enforce, and they are deterministic. Nanoseconds are recorded for telemetry and nothing branches on them. Keeping the clock out of control flow is what lets the simulator run the real scheduler on a virtual clock and get the same trace every time.

Finally there is `yieldCheck()` and a stall watchdog. Cooperative scheduling has one classic failure, the task that never yields, and capabilities do nothing about it. So the tick stops enforcing and starts reporting: if the running task has passed no yield point since the last tick, the supervisor receives a fault naming it. That turns "we are careful about yielding" from a convention into something measured.

## Reactors, and what swapping them proved

Five backends have been written against the same contract: poll, epoll, io_uring readiness, io_uring completion, and kqueue. `sched.zig` stayed byte-identical through all of them. That is the strongest evidence I have that the separation holds.

The kqueue port reused the reactor whole. Of `reactor.zig`'s 400 lines, about 15 were epoll-specific, and those 15 are now `backend_epoll.zig` and `backend_kqueue.zig`. The client table, the byte accounting, and the round policy stay shared. The semantics line up because `EV_ONESHOT` and `EPOLLONESHOT` mean the same thing: an event fires, the registration is gone, and the task stays parked until something rearms it.

That interchangeability covers the readiness backends. The io_uring completion build asks for a different application shape, since there the kernel fills buffers and hands back completions, so `app/server_uring2.zig` is its own file. The portability claim covers the readiness backends and stops there.

## Determinism

`tests/sim.zig` imports the unmodified scheduler and runs it on a virtual clock. The same seed gives a byte-identical trace hash over a million dispatches, and six hours of virtual time takes seconds of real time.

The hash also matches across macOS on arm64 and Linux on x86_64. That falls out of keeping the clock away from control flow. Once no branch reads the time, the platform has nothing left to influence.

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
docs/        APIGUIDE.md, LESSONS.md, architecture diagram
results/     CSVs and plots from the original measurements
stages/      srv..srv14, every checkpoint, kept as history
```

## Build and run

Requires Zig 0.16.0. Use `-Drelease`, which Zig 0.16 exposes in place of `-Doptimize`.

```sh
zig build -Drelease        # servers and the example into zig-out/bin
zig build echo             # run the example server
zig build test             # six test programs
zig build check            # typecheck everything for Linux without running
```

|  | Linux | macOS |
|---|---|---|
| `zig build test` | 6/6 | 6/6 |
| `examples/echo.zig` | epoll | kqueue |
| `app/server.zig` | epoll | kqueue |
| `app/server_uring2.zig` | io_uring | not available |
| `zig build bench` | yes | not ported |

The Linux build links no libc. Nothing calls `linkLibC`, the ELF is static, and it carries no libc symbols, so a binary cross-compiled on a Mac runs on a Linux box with nothing installed. macOS is the opposite and always will be, because there is no stable syscall ABI there.

`zig build check` cross-compiles every executable for `x86_64-linux-gnu` without running it, which is how a change made on a Mac gets checked against the Linux path.

## Status

Working: the scheduler, all five reactors, supervisors, cancellation, the timer wheel, the parser, the simulator, the buffer pool, and the control surface.

Open, roughly in the order I would take them:

- One carrier, single threaded. Multi-core is designed as sharding, and still on paper.
- Tasks are hand-rolled state machines. Fibers, `perform` and `around` are still open, and are the part most likely to change how this feels to use.
- The io_uring completion build stays flat as queue depth rises. A provided buffer ring gives global backpressure, so per-connection flow control has to come from somewhere else. See APIGUIDE.
- `iobuf` needs a "the kernel owns this" state before an IOCP port is possible.
- The benches speak raw Linux syscalls, so generating load on macOS needs an outside tool such as wrk.
- `bench/diskbench.zig` uses a raw `open` syscall, so it compiles for x86_64 Linux only. arm64 is openat only.

Numbers from the original measurements live in `results/` and are discussed in `docs/LESSONS.md`. Read them as a diary of one session on one shared vCPU with the load generator running on the same core.

### A kernel bug worth knowing about

Ubuntu 6.8.0-136 and -137 return `EINVAL` from `io_register_pbuf_ring` when the `io_uring_buf_reg` reserved fields are correctly zeroed, and succeed when they are not. The check is inverted, so every caller that follows the spec, liburing and Zig's standard library included, cannot register a provided buffer ring at all. That takes `app/server_uring2.zig` and `bench/gen.zig` with it.

It is [Launchpad #2162843](https://bugs.launchpad.net/ubuntu/+source/linux/+bug/2162843), still open. The cause is a mangled backport of upstream commit `1724849`, which rewrote `if (reg.resv[0] || reg.resv[1] || reg.resv[2])` as `if (!mem_is_zero(reg.resv, sizeof(reg.resv)))`. `mem_is_zero` is `memchr_inv(s, 0, n) == NULL`, so upstream compiles to a `jne` into the error path. The shipped kernel has a `je`. The negation was lost on the way in, and disassembling the function out of `/proc/kcore` is what confirmed it.

`src/uring_bufring.zig` works around it. It registers the ring correctly first and falls back to the inverted form only on `EINVAL`, so a correct kernel succeeds on the first call and stays on the spec-correct path. When the fallback runs, the server says so in its stats. 6.8.0-134 is the last version known to be unaffected.

## Reading order

1. `docs/LESSONS.md`. What went wrong and what each thing taught. Start here.
2. `examples/echo.zig`. The smallest thing that uses the kernel.
3. `docs/APIGUIDE.md`. How the pieces fit, and every knob.
4. `src/sched.zig`. The core, and the one file that touches no I/O.
