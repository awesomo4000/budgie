# budgie

<img src="img/budgie-small.png" alt="budgie" width="180" align="right">

An experiment: build an async runtime by copying the architecture of a microkernel instead of the architecture of a promise library.

## Observations about runtimes

If you stare at an asynchronous runtime system long enough it begins  to look like a small operating system. An asynchronous runtime divides a program into small pieces so that they can run in any order, which allows concurrent execution of those pieces and gives a program the ability to multiplex many operations at once. That freedom to run pieces as separate tasks is also what allows parallelism, by executing the pieces on different threads.

Numerous asynchronous systems exist, with different features, execution models, and multiplexing methods. A runtime handles tasks such as managing network and file descriptors, timers, background tasks, long running cpu bound tasks, and signals. These features must also work across differing event models such as "readiness-based" events, as well as "completion based" events.

Perhaps the most important consideration that a runtime makes many thousands of times per second is: given everything that could run right now, what runs next?

The piece that decides what should run is a scheduler. In many async runtimes we do not call it that. We call it an executor, a reactor, a task queue, or a disruptor, and we give it an API shaped like function calls so it feels like ordinary code. The kernel underneath is real either way. 

## The idea

Operating systems people have been at this for fifty years, and one branch of that work went somewhere unusual. seL4 is a microkernel small enough that its implementation has been proved to match its specification. It flies drones and sits inside medical devices. It is about ten thousand lines.

The thought that started this project was to open the hood of seL4, look at its architecture, and attempt to use its ideas and constructions to form an asynchronous runtime usable in ordinary user processes on common operating systems. We're experimenting with the seL4 architecture and primitives.

In seL4, a "thread" there gets a *scheduling context*: a budget and a period. The kernel enforces the thread's scheduling using those two numbers, and maintains separation so that the kernel does not know much else about the thread. Memory comes from "untyped capabilities" that the process can use, while the kernel tracks the derivation tree of memory, and keeps the meaning of the memory with the requestor who asked for it. In both cases the kernel holds an accounting structure, but not a semantic one.

Apply that to a runtime and you get a scheduler whose entire vocabulary is task ids, priorities, budgets, deadlines, and a cancel bit. It holds no file descriptors, performs no syscalls, and cannot read the clock. It has no representation of a request, a connection, or a protocol.

## Good idea?

I do not know yet. That is what building this project is for.

What is here is a "I/O free" cooperative scheduler with budgets and cancellation, and a reactor that does know about I/O and nothing about scheduling policy. The arrangement of the pieces here is similar in architure to a microkernel.

What I wanted was a read on how it *feels* to write programs this way, next to the alternatives:

- **async/await** colours your functions and hands back cancellation as a value, so any `catch` along the path can swallow it.
- **Goroutines and channels** give you cheap concurrency and no answer at all to "this handler is eating the machine".
- **BEAM processes** are the closest relative and got there decades earlier: isolation, preemption, supervision trees. Erlang solved the runaway task by preempting it. This meters instead, and stays cooperative, which is a weaker guarantee bought at a lower price.
- **Zig's `std.Io`** threads an explicit `Io` through every caller, a different and defensible answer to the same question about who owns effects.

Against those, what differs here is that a budget is a number the scheduler holds, so a budget overrun is something the runtime measures.

## What it looks like to use

`examples/echo.zig` is about 120 lines whose only imports are this kernel and `std`. The loop is the whole contract:

```zig
while (true) {
    while (s.popRunnable()) |t| step(t);   // run what is runnable
    _ = r.wait(&s, 1000);                  // block until an fd is ready
    s.expire(nowMs());                     // fire expired timers
}
```

The `s` and `r` used above are the two values the program owns, declared once at file scope, along with whatever state the application keeps per task:

```zig
var s: sched.Sched = .{};                        // the scheduler
var r: Reactor = .{};                            // epoll, kqueue or IOCP, chosen at compile time
var conns: [max_conns + 1]Conn = @splat(.{});    // this program's own per-task state
```

A task is a state machine the scheduler hands control to. A task is formed by initializing the pieces it needs:

```zig
const t = freeTask() orelse { sys.close(fd); break; };

conns[t] = .{ .fd = fd };   // the application's state for this task
s.live[t] = true;           // the scheduler will now consider it
s.setPrio(t, 2);            // below the listener, above background work
s.admit(t, 1000, 50);       // 1000 units of budget, 50 held back for cleanup
r.open(t);                  // the reactor starts accounting for it
r.watch(t, fd, .read);      // park it until the descriptor is readable
```

There is no task object here, and no allocation. `t` is a small integer, and each of those calls writes into a table indexed by it. The scheduler holds the budget, the priority and the deadline. The reactor holds the descriptor and the byte accounting. `conns` holds whatever this particular program needs, and the scheduler never looks at it. Ending a task is those same lines in reverse, which is what `finish` does below.

Once admitted, a task runs until it would block, parks itself against a descriptor, and returns:

```zig
fn step(t: TaskId) void {
    if (t == listener_task) return accept();     // task 0 is the listener
    const c = &conns[t];
    if (c.writing) return write(t, c);           // mid-response, finish it
    if (drain(t, c)) return;                     // a request may already be buffered

    const n = r.read(t, c.fd, c.in[c.in_len..]);
    if (n < 0) return r.watch(t, c.fd, .read);   // would block, park again
    if (n == 0) return finish(t, c);             // peer hung up
    c.in_len += @intCast(n);

    if (drain(t, c)) return;
    r.watch(t, c.fd, .read);                     // parser wants more bytes
}
```

That is the whole function, copied out of `examples/echo.zig`. `drain` feeds whatever is buffered to the parser and returns true if the connection changed phase. `respond` formats into `c.out` and then calls `s.makeRunnable(t, .spawn)` rather than parking, because it has work to do and needs a turn rather than a wakeup. `write` empties `c.out` and parks on `.write` if the socket fills. `finish` unwatches, releases the budget, and closes the descriptor.

The `drain` before the read is the part worth pausing on. A pipelined client sends its next request without waiting for the previous answer, so `c.in` can already hold a complete request that no amount of further reading will announce. Calling `read` first gets `EAGAIN`, parks the task on `.read`, and the connection stalls until the peer sends something else or gives up. I had this wrong until testing four pipelined requests returned one answer.

Two things in there are important to notice:

`r.read` is the *reactor's* call, not the task's. A readiness reactor that hands out `watch` and lets the application do the read never learns how many bytes anyone consumed, so any fairness policy has to live somewhere else and coordinate with it. Six attempts came apart that way before the read moved inside. Now the reactor sees the byte count, so it owns the accounting and the arming together, and a pause happens in the same code that will resume it.

The second is that the file names no operating system. Build it on Linux and the reactor underneath is epoll. Build it on macOS and it is kqueue. Build it on Windows and it is IOCP. The same file compiles for all three. `zig build echo`, then `curl localhost:8080`.

## What the scheduler holds

The scheduler keeps strict priority classes, and it selects the highest non-empty one with `@ctz` on a bitmask.

It tracks three budget levels, which answer different questions. A per-request `cap` bounds one unit of work. The `budget` is what a task currently holds. Above both sits a supervisor tree, where `topUp` deducts from every ancestor, and that third level is the interesting one because it makes "all connections together may spend X per period" expressible.

Each task also carries a cleanup reserve that only the unwind path can spend, so releasing a connection stays funded even after the work that overran it.

Cancel tokens are generational, and cancellation lives in scheduler state rather than travelling as a return value, so it survives whatever error handling the task does on the way out.

Timers live in an intrusive wheel that arms, disarms, and rearms in constant time.

The scheduler counts two currencies and keeps them apart. Work units enforce, and they are deterministic. Nanoseconds are recorded for telemetry and nothing branches on them. Keeping the clock out of control flow is what lets the simulator run the real scheduler on a virtual clock and get the same trace every time.

Finally there is `yieldCheck()` and a stall watchdog. Cooperative scheduling has one classic failure, the task that never yields, and capabilities do nothing about it. So the tick stops enforcing and starts reporting: if the running task has passed no yield point since the last tick, the supervisor receives a fault naming it. That turns "we are careful about yielding" from a convention into something measured.

## Reactors, and what swapping them proved

Six backends have been written against the same contract: poll, epoll, io_uring readiness, io_uring completion, kqueue, and IOCP. `sched.zig` stayed byte-identical through all of them. That is the strongest evidence I have that the separation holds.

The kqueue port reused the reactor whole. Of `reactor.zig`'s 400 lines, about 15 were epoll-specific, and those 15 are now `backend_epoll.zig` and `backend_kqueue.zig`. The client table, the byte accounting, and the round policy stay shared. The semantics line up because `EV_ONESHOT` and `EPOLLONESHOT` mean the same thing: an event fires, the registration is gone, and the task stays parked until something rearms it.

IOCP was the one that tested the contract rather than confirming it, because IOCP is not a readiness interface. epoll and kqueue answer which sockets you could act on now; IOCP answers which operations you already started have finished. The bridge is the zero-byte overlapped operation: a `WSARecv` for zero bytes completes when the socket becomes readable without consuming anything. So the same 400 lines of fairness policy sit on top, unchanged, and `backend_iocp.zig` is the platform half.

Two things there are genuinely different rather than differently spelled. A listening socket has no readiness at all, so accept goes through `AcceptEx`, which does not report that a client arrived but performs the accept. And the kernel owns an operation's `OVERLAPPED` block until its completion packet has been collected, which cancelling does not shorten: `CancelIoEx` asks, the packet still arrives marked cancelled. So a cancelled block is held rather than freed, and freeing it early would be a use-after-free the kernel commits on your behalf at a time it picks. `tests/iocp_orphan_test.zig` does nothing but cancel, in the three shapes that leave the kernel holding different things, and asserts the conservation law.

That interchangeability covers the readiness backends. The io_uring completion build asks for a different application shape, since there the kernel fills buffers and hands back completions, so `app/server_uring2.zig` is its own file. The portability claim covers the readiness backends and stops there.

The claim is now checked rather than asserted, which it was not until recently. `app/server.zig` builds against epoll, kqueue, IOCP and io_uring readiness with nothing in it changing, and the same socket tests run against all of them. That matters because for a while it was false and nothing said so: nothing built against the readiness io_uring reactor, so when the server grew `open`, `close`, `read` and a byte-fairness interface, that reactor silently stopped fitting. An alternative nobody exercises does not rot quietly on its own. The interface moves and the alternative stops being an alternative, which is worse, because the portability claim is the thing the alternatives exist to support.

Having all three also gives the control that separates two questions usually asked as one: whether io_uring is faster because of the submission interface, or because of completion semantics. Same server, three reactors, one difference at a time.

## Determinism

`tests/sim.zig` imports the unmodified scheduler and runs it on a virtual clock. The same seed gives a byte-identical trace hash over a million dispatches, and six hours of virtual time takes seconds of real time.

The hash also matches across all three platforms. Measured over 72 runs each of macOS on arm64, Linux on x86_64 and Windows on x86_64, being 24 seeds times three task counts, nine traces per run: identical, byte for byte, with the three result files sharing one MD5. That falls out of keeping the clock away from control flow. `sim` imports `sched`, `quota`, `clock` and `invariant` and nothing else, and the one call it makes to the wall clock times the run for reporting and feeds no decision. Once no branch reads the time, the platform has nothing left to influence.

It is worth being clear about what that does not cover. `sim` never touches a reactor, so it says nothing about IOCP against epoll against kqueue. The socket tests and the seeded chaos runs are what cover those, and they pass on all three.

## Layout

```
src/
  sched.zig            the scheduler. Pure logic, I/O free.
  quota.zig            conservation trees over a resource
  accounts.zig         per-phase telemetry, labels defined by the application
  reactor.zig          readiness reactor: client table, byte accounting, rounds
  backend_epoll.zig    the epoll half of it (Linux)
  backend_kqueue.zig   the kqueue half of it (macOS)
  backend_iocp.zig     the IOCP half of it (Windows), over zero-byte overlaps
  invariant.zig        one shape for "something that should be true is not"
  interest.zig         what a task waits for
  reactor_uring.zig    io_uring, IORING_OP_POLL_ADD (readiness)
  reactor_uring2.zig   io_uring, multishot recv and a buffer ring (completion)
  uring_bufring.zig    buffer ring registration, around a broken kernel
  http.zig             sans-I/O request parser: bytes in, events out
  iobuf.zig            generational pool of I/O buffers
  drr.zig              deficit round robin over byte flows, off by default
  clock.zig            real or virtual time, injected
  sys.zig              sockets and process introspection, per platform
  sys_linux.zig        its Linux half
  sys_darwin.zig       its macOS half
  sys_windows.zig      its Windows half, over ws2_32 and ntdll directly
  root.zig             module root

app/         two servers. `server.zig` builds against epoll, kqueue, IOCP or
             io_uring readiness; `server_uring2.zig` is the completion one
examples/    echo.zig, and cancellation with and without a dispatcher
tests/       parser, pipelining, chunk fuzzing, the simulator, and socket
             tests over every backend: deadlines, pool exhaustion, the write
             path, cancellation, and a seeded chaos run
bench/       load generators and microbenchmarks
docs/        APIGUIDE.md, LESSONS.md, architecture diagram
results/     CSVs and plots from the original measurements
stages/      srv..srv14, every checkpoint, kept as history
```

## Build and run

Requires Zig 0.16.0. Use `-Drelease`, which Zig 0.16 exposes in place of `-Doptimize`.

```sh
zig build -Drelease        # servers and the example into zig-out/bin
zig build echo             # run the example server
zig build test             # 16 test programs, 28 runs on Linux
zig build check            # typecheck everything for Linux without running
```

|  | Linux | macOS | Windows |
|---|---|---|---|
| `zig build test` | 28/28 | 16/16 | 16/16 |
| `examples/echo.zig` | epoll | kqueue | IOCP |
| `app/server.zig` | epoll | kqueue | IOCP |
| `app/server_uring2.zig` | io_uring, socket-tested | not available | not available |
| `zig build bench` | all eight | the four network ones | the four network ones |

Linux runs more programs than the other two because the socket tests repeat over the io_uring readiness reactor. `iocp_orphan_test` compiles everywhere and reports that there is nothing to test off Windows, which is cheaper than a build condition and means it cannot rot unnoticed.

The Linux build links no libc. Nothing calls `linkLibC`, the ELF is static, and it carries no libc symbols, so a binary cross-compiled on a Mac runs on a Linux box with nothing installed. Windows links no libc either: `std.os.windows` in 0.16 ships `ws2_32` as types with no function declarations and `kernel32` with a single extern, so the externs are ours, declared against ws2_32, kernel32 and ntdll. macOS is the opposite and always will be, because there is no stable syscall ABI there.

`zig build check` cross-compiles every executable for `x86_64-linux-gnu` without running it, which is how a change made on a Mac gets checked against the Linux path. `zig build -Dtarget=x86_64-windows` does the same for Windows, and both are worth running before pushing, because a platform seam breaks at compile time or not at all.

Two Windows differences are not spelled differently, they behave differently, and both cost a bug before they were found. Closing a socket that still holds unread data is an abortive close, so `sys_windows.close` half-closes first: measured, plain close delivered 0 bytes of a 78-byte response and `shutdown` first delivered all 78. And the default timer resolution is 15.625ms, which the completion-port wait inherits, so a 40ms deadline was answered at 47.6ms until the platform layer asked for 1ms. It is now answered at 40.4ms, against 41.0ms on macOS.

## Status

Working: the scheduler, all six reactors, supervisors, cancellation, the timer wheel, the parser, the simulator, the buffer pool, and the control surface.

Open, roughly in the order I would take them:

- One carrier, single threaded. Multi-core is designed as sharding, and still on paper.
- Tasks are hand-rolled state machines. Fibers, `perform` and `around` are still open, and are the part most likely to change how this feels to use.
- The io_uring completion build stays flat as queue depth rises. A provided buffer ring gives global backpressure, so per-connection flow control has to come from somewhere else. See APIGUIDE.
- `gen`, `diskbench`, `iobench` and `sysc` stay Linux-only. The first three are io_uring, and `sysc` measures raw syscall entry cost, which on macOS would be measuring libc.

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
