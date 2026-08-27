# API guide

Four layers, each ignorant of the ones beside it.

```
sched.zig     runnability, budgets, deadlines, cancellation   (no fd, no clock)
reactor*.zig  fd readiness or completions                     (no budget, no phase)
http.zig      bytes -> events                                 (no fd, no allocator)
server*.zig   drives all three
```

The separation is load-bearing: `sched.zig` was byte-identical across a poll port, an epoll port, an io_uring readiness port and an io_uring completion port.

---

## The kernel step

The whole scheduler loop, from `server.zig`:

```zig
while (a.s.popRunnable()) |t| a.run(t);       // O(ready), not O(max_tasks)
const now = a.clock.ms();
a.s.refillSups(now);
const timeout = if (a.s.anyRunnable()) 0
                else if (a.s.timeoutMs(now)) |ms| @min(1000, ms)
                else 1000;
_ = a.r.wait(&a.s, timeout);                  // reactor marks tasks runnable
a.s.expire(a.clock.ms());
```

The completion-mode server (`server_uring2.zig`) adds an inner loop that alternates "convert completions into work" and "drain the work" until neither remains, and only then blocks. Getting that order wrong costs a syscall per request — see LESSONS.md.

---

## sched.zig

### Tasks

```zig
pub const TaskId = u32;
pub const max_tasks = 8192;
pub const Wake = enum { spawn, io, deadline, cancelled };
```

```zig
s.setPrio(id, prio);              // 0 = highest, prio_idle = lowest
s.assignSup(id, sup);             // which supervisor this task draws from
s.admit(id, cap, reserve);        // cap is the PER-REQUEST ceiling
s.release(id);                    // invalidates every outstanding token
```

`admit` sets `budget = 0`. Units only ever enter a task through `topUp`, so the supervisor tree stays conservative.

### Runnability

```zig
s.makeRunnable(id, why);          // enqueue in the task's priority class
s.popRunnable() ?TaskId;          // highest non-empty class, FIFO within it
s.anyRunnable() bool;
s.reasonFor(id) Wake;             // why this task woke
s.runnableAbove(class) bool;
```

Dispatch is by priority; budget bounds the turn. Both must pass: a task runs iff its class is the highest non-empty **and** it has budget.

> **Gotcha.** In a single-threaded cooperative runtime, budget is the *only*
> mechanism that makes a task stop being runnable, and being not-runnable is
> the only way the thread can sleep. An always-runnable idle-priority task
> keeps `anyRunnable()` true forever, so `timeout` is always 0 and the process
> never yields the core. Measured: 45k req/s instead of 129k, and time asleep
> collapsing from 80% to 2.3%.

### Budgets and supervisors

```zig
s.chargeTo(id, account, units) bool;  // false -> transition to cleanup
s.chargeReserve(id, units);           // the unwind path only
s.renewCap(id, cap);                  // new request on an existing task
s.setReserve(id, reserve);

s.defineSup(id, parent, quota, period_ms, tag);
s.topUp(id, want) bool;               // deducts from every ancestor, all-or-nothing
s.refillSups(now_ms);
s.nextRefillMs(now_ms) ?i64;
```

Three distinct levels, and they answer different questions:

| level | bounds |
|---|---|
| `cap` | one request |
| `budget` | the grant currently held |
| supervisor | a whole class, across all its tasks |

Only the third can express "all connections together may spend X per period", and that is not derivable from per-task fields.

> **Gotcha.** `sup_of` defaults to the **root** supervisor, and an explicit
> `no_sup` is a denial. An earlier version treated `no_sup` as *unmetered*, so
> one forgotten `assignSup` gave a background task infinite budget: 200,768
> iterations at `bg_budget=0`, 78% of the machine, throughput 126k -> 46k. An
> admission mechanism whose default is "unmetered" grants infinity to exactly
> the task nobody configured. Fail closed.

### Cancellation

```zig
const tok = s.cancelTok(id);      // generational
s.cancel(tok) bool;               // false if the slot was recycled
s.isCancelled(id) bool;
```

`cancel` does two independent things:

- **safety** — `budget = 0` and `cap = 0`, so the task structurally cannot do more body work even if the app's handling is buggy. The reserve is untouched.
- **liveness** — `makeRunnable(.cancelled)`, so a task parked in epoll notices now rather than at its deadline.

Sticky, idempotent, and **not a value anything can swallow**. The check belongs in `run()`, unconditionally, before the phase dispatch:

```zig
if (c.phase != .cleanup) {
    if (a.s.isCancelled(t)) return a.enterCleanup(t, c, .cancelled);
    if (a.s.reasonFor(t) == .deadline) return a.enterCleanup(t, c, .deadline_missed);
}
```

> **Gotcha.** Per-phase checking is the same shape of mistake as per-call-site
> `catch`. An earlier version checked only in phases that called `charge`, so a
> cancelled task in `.writing` — which never charges — kept writing forever on
> a zeroed budget.

### Deadlines

```zig
s.arm(id, at_ms);      // O(1)
s.disarm(id);          // O(1)
s.timeoutMs(now) ?i64; // how long the caller may block
s.expire(now);         // fire everything due
```

Single-level timer wheel, `wheel_slots` buckets at 1 ms, intrusive nodes in the task array, occupancy bitmap so "when is the next timer" skips empty buckets 64 at a time. An overflow list handles deadlines beyond one revolution.

Because arm/disarm are O(1) with no stale entries, no slack window is needed. The heap version this replaced discarded 500,532 stale entries in one run.

> **Gotcha.** Sweep the overflow list *before* the fire loop, and fire directly
> rather than relinking when an entry is already due — relinking a past-due
> deadline puts it in a slot the hand has passed, which wraps a full revolution
> into the future.

### Two currencies

```zig
pub const Account = enum { accept, parse, work, write, cleanup, background };
pub const observe_ns = true;   // compile out every clock read on the charge path

s.chargeTo(id, .parse, units);   // ENFORCED, deterministic
s.observe(.parse, ns);           // OBSERVED, never read by control flow
```

Units enforce so budget exhaustion lands at the identical point on every replay. Nanoseconds calibrate units to latency, detect drift in ns-per-unit, and — most usefully — expose wall time that charges nothing at all. In a typical run that unaccounted fraction is **96%**: syscalls, the reactor, the scheduler. That is the number that tells you tightening budgets will not find your next win.

Measured cost of carrying the ns currency: none. 212,654 req/s with it, 198,905 without.

---

## Reactors

All four implement the same surface:

```zig
r.watch(task, fd, .read | .write);
r.unwatch(task);
r.watching(task) bool;
r.wait(&sched, timeout_ms) usize;   // marks ready tasks runnable
```

`reactor_uring2.zig` adds a completion queue, because it reports data rather than readiness:

```zig
r.armRecv(task, fd);          // ONE multishot submission per connection lifetime
r.submitSend(task, fd, bytes);
r.next() ?Completion;         // { task, kind: data|eof|err|writable|sent, data }
r.release(comp);
```

That is the interface break worth knowing about before committing to an effect vocabulary: **`perform Read(fd, buf) -> n` maps onto readiness and cannot express submit-N/complete-N.** A completion-based runtime wants `perform Recv() -> (buf, len)`, where the buffer comes *from* the operation.

### Completion keys must be generational

```zig
const gen_shift: u6 = 20;
fn key(tag: u64, task: TaskId, gen: u32) u64;
```

A completion can be queued before the task that owns it is released. If the slot is reused first, its bytes are delivered to whoever inherited it. On Linux that needs a close/reuse race; **on IOCP it is the normal shutdown path**, because `CancelIoEx` does not complete synchronously and a cancelled `WSARecv` still posts a completion with `ERROR_OPERATION_ABORTED`. Costs nothing measurable; A/B was within noise.

### What "unfair" actually turned out to mean

The fairness work below was chasing a badly chosen metric, and the correction is worth reading before any of it.

**Share of total requests served is not a fairness metric** when clients differ in concurrency. A client with 256 requests in flight completes more than one with 1 in flight; that is Little's Law, not starvation. Measured, polite (depth 1) against greedy (depth 256), fairness OFF:

```
polite   20,964 req/s   p50  0.327ms   p99  19.9ms
greedy  124,590 req/s   p50 15.97ms    p99 101.7ms
```

The "starved" client has **48x better median latency**. It is served in ~0.3ms every time it asks; it simply asks less often. And enabling the fairness cap drives the greedy client's p50 to 102ms to move a share number that was not measuring service quality.

The right question is whether the greedy client HARMS the polite one:

```
polite alone      91,970 req/s   p50 0.280ms   p99  1.09ms
polite + greedy   17,141 req/s   p50 0.389ms   p99 19.10ms
```

p50 is essentially unaffected. The real harm is the **tail**, and it is not queueing behind the other client's work -- it is the drain bound:

```
tick    40ms  20ms  10ms   5ms   2ms
p99     23.5  19.7  10.6   6.7   5.0    bounded_drain=1
p99     23.5    --    --    --  22.6    bounded_drain=0
```

Almost linear with the tick, and flat without the drain bound. `bounded_drain` is the mechanism, the tick is its resolution, and raw throughput is unchanged across the whole range (91-103k). **The default tick is now 2ms**, which takes the competing client's p99 from ~19.7ms to ~5ms and raises its throughput too.

One knob, already present, no new machinery -- against a fairness subsystem that cost six failed attempts and 30k req/s.

### Byte fairness as a passive server (reactor client table)

The reactor keeps a PRIVATE per-client table -- deficit, throttled, backlogged -- and owns the `read`. That single change is what made byte fairness work on epoll after six failures with a shared `drr.zig`.

Two things had to be true, and only the second is obvious in hindsight:

**1. One owner.** A readiness reactor that hands out `watch` and lets the caller read has no idea how much anyone consumed, so accounting lived in the application and enforcement (arming) lived in the reactor. Neither owned "has a round elapsed", which is why every round policy failed differently. Now the reactor does the syscall, so it sees the bytes, so it owns both halves. Pause and resume cannot race because they are the same code.

**2. A round rate the quantum cannot influence.** Advancing a round when the loop is about to block makes the rate a function of pending work, which is a function of the quantum -- the knob cancels itself. Measured with the reactor already owning everything: quantum 1024 -> 16384 moved `rounds` 1088 -> 305 and the fairness share not at all (6-10% throughout). Advancing on the TICK gives `quantum / tick_period`, which is controllable.

Measured, polite(32 conns, depth 1) vs greedy(32 conns, depth 256), four interleaved trials each:

```
off  8.4%  7.8%  6.3%  5.9%
on  37.9% 37.7% 37.7% 37.6%     (quantum 1024, tick 20ms)
```

Ranges nowhere near overlapping, and the "on" numbers are stable to one decimal place -- `throttles` and `resumes` match exactly every run. Throughput cost 91.5k -> 89.2k.

Not yet 50%: the greedy client is rate-capped at `quantum/tick` while the polite one is under its cap and unconstrained, so this is a cap rather than a proportional share.

**Self-tuning was attempted (`drr_quantum=-1`) and is not usable.** It reaches a fair share -- 48.3%, stable to 0.1 across trials -- and pays 15x throughput for it: 6.3k req/s against 97.8k unthrottled and 74k at a fixed quantum of 1024.

The blocker is **work conservation**, not the estimator. Every client is credited `quantum` per tick whether it wants it or not, and unused allowance is discarded rather than redistributed, so a polite client leaves most of its share on the floor and nobody picks it up. Dividing by the demanding set instead of by every client is the obvious fix and did not move the number, which says redistribution has to happen WITHIN a round rather than by adjusting the divisor between rounds.

Three estimator bugs were found and fixed on the way, each worth avoiding:

* **peak-hold on a bursty signal** latches 10-30x above the mean -- quantum came out at 1670-6920 where ~170 binds.
* **an EWMA of throughput you are limiting is a closed loop.** It fed its own suppression back in, walked the quantum to the floor, and collapsed throughput while the share looked beautiful. Only sample rounds where you throttled nobody: those are demand-limited and honest.
* **the estimate starts at zero**, so the first round clamps the quantum to the floor, everything throttles, and no honest sample is ever taken again. Needs a warmup before it is trusted.

**Service-counted rounds were also attempted (`service_rounds=1`) and do not work.** The idea: `epoll_wait` already reports the backlogged set, which should supply the service-counted round classic DRR needs. Two failures:

* counting throttled clients as backlogged **deadlocks** -- they are exactly the ones we refuse to serve, so `served >= backlogged` never holds. 8 rounds in 5 seconds, everything stalled.
* counting only the ready set gives **no fairness at all** (3.5-6.7% share against 7.2% with fairness off). The ready set is "who has data at this instant", not "who is competing": with 64 clients and fast service only a handful are ready at once, so a round completes after a few reads and credits everyone almost continuously.

Both leave the round rate emergent from the reactor's wake pattern, which is the same root cause the tick was introduced to fix.

**The fixed quantum remains the recommended operating point**, and the open problem is now stated precisely: what is needed is a notion of "competing" that spans time, rather than an instantaneous readiness snapshot or a capacity estimate derived from throughput you are limiting.

### Byte fairness (drr.zig, superseded on epoll)

Three currencies, three shapes:

| | shape | who decides | exhaustion means | enforced at |
|---|---|---|---|---|
| compute units | rate (refills on a timer) | you | wait for the period | `chargeTo` |
| buffers | level (returns on release) | you | terminal -- 503 | `acquire` |
| **bytes** | **flow** | **the peer** | throttle the source | **recv arming** |

Bytes are the only one the peer controls, which makes them the only account that can bound a greedy client -- capping compute after the bytes have landed is closing the barn door.

`drr.zig` is deficit round robin: one integer per flow, O(1), provably max-min fair. Credit active flows one quantum per round; RESET quiet flows rather than crediting them, so an idle flow cannot bank allowance and burst on return. Enforcement is `pauseRecv` -- cancel the multishot so the kernel stops reading and the peer's window closes.

Measured, polite (32 conns, depth 1) vs greedy (32 conns, depth 256), five interleaved trials each:

```
off : mean 22.0%  median 19.7%  range 14.3-33.5
on  : mean 59.2%  median 60.3%  range 46.8-68.0
```

Non-overlapping. A solo client is unaffected (86k req/s, work-conserving).

> **Gotcha.** `idle` must mean "quiet for a whole round", not "finished a
> request". Calling it whenever the buffer drained reset the deficit after
> every request and DRR never engaged at all -- `throttles=0` for hours.

> **RESOLVED ON THE READINESS BACKEND** by moving the accounting into the
> reactor's private client table (see below). The text that follows describes
> the shared-`drr.zig` design and why it failed; kept because the failure modes
> are instructive.
>
> **NOT PORTABLE BETWEEN BACKENDS.** Enabled by default on the completion
> reactor only. The policy and the enforcement are both fine on epoll (do not
> re-arm the read interest, which is cheaper than io_uring's cancel) but the
> TUNING does not transfer. polite(depth 1) vs greedy(depth 256), polite's
> share of a fair 50%:
>
> ```
> quantum   16384   24576   32768   49152   65536
> epoll       97%     93%   65-92%  31-52%    34%
> io_uring  58-67%  (stable across trials)
> ```
>
> A narrow, unstable window on epoll with no setting reliably near 50%.
> Matching the charge granularity (capping the read to 2 KB, the completion
> buffer size) did not unify them -- it moved epoll to 8%.
>
> Root cause: DRR's effective rate is `quantum x round_rate`, and round_rate
> is emergent from each backend's wake pattern rather than a controlled
> parameter.
>
> **The seam exists** -- `drr.RoundPolicy`, with the deficit accounting shared
> and only the round boundary per backend. `on_block` works and is the
> default. `on_service` (textbook DRR: a round ends when every backlogged flow
> has had a turn) is the right idea and the implementation here does NOT work,
> in two distinct ways, both measured:
>
> * comparing against the live `n_active` is trivially true after a round
>   clears `active`, so a round fires per charge: 15,014 rounds, zero
>   throttles, no effect.
> * snapshotting the round size instead deadlocks -- a throttled flow cannot
>   be charged, so the count never reaches the snapshot, rounds stop at 1, and
>   nothing resumes. Throughput 55k -> 3k.
>
> What is missing is a notion of "backlogged" that survives throttling: a flow
> with bytes waiting is still owed a turn even though it cannot be served.
> That needs the reactor to report "this flow has pending data", which
> `drr.zig` deliberately cannot see. Resolving that is the real work, and it
> is where a genuinely per-backend implementation would go.

> **Gotcha.** Advancing a round only when EVERY active flow is stuck sounds
> right and starves: with many connections some are always mid-flight, so
> rounds never advance and paused flows never resume. One client measured at
> exactly zero. Advance whenever anything is waiting on credit.

### -ENOBUFS terminates a multishot recv

The single worst bug in this file. **A multishot recv ENDS on -ENOBUFS.** If nothing re-arms it, the connection is dead forever: it is not runnable, so no task ever runs to notice, and a peer waiting on a response waits for good.

It presented as a *bimodal fairness result* -- one client getting exactly 0 requests in some trials and a fair share in others, depending on whether the buffer ring ran dry at the wrong moment (`enobufs=52` in the failing runs, 20 in the healthy ones). It was initially misattributed to DRR, which was disabled in the failing runs.

Re-arming is deferred to the top of the next kernel step, not done on the spot: on the spot the ring is still empty and it would terminate again in a tight loop. By the next step the completions in hand have been processed and their buffers returned.

### Backpressure, and why completion mode does not get it free

Readiness mode has flow control by construction: `read(fd, b.in[b.in_len..])` takes **only what fits**, and the rest stays in the socket buffer. The peer's window closes on its own.

Completion mode has none. By the time you hear about a multishot recv the kernel has *already read the bytes*, so the app either takes them or loses them.

The obvious fix -- refuse the completion and keep its ring buffer out of the ring -- does not work per connection, and this is worth knowing before designing around it:

> **A provided buffer ring is a GLOBAL resource.** Holding a buffer does not
> stop the kernel delivering to *that* socket; multishot recv simply uses a
> different buffer. It shrinks the shared pool, throttling every connection
> rather than the greedy one. Measured: backpressure engaged (32 events) and
> throughput collapsed to 496 req/s, because the held buffer could only be
> returned once its connection had fully drained.

What works instead:

1. **Size the inbound buffer so a single completion always fits** -- `>= uring_buf_size + max_request_bytes`. Then refusing a completion never arises from one arrival.
2. **Treat real accumulation as the resource limit it is.** Reaching the limit now means the peer is sending faster than we serve while the connection is busy. That is a 503 (`.overloaded`), not a 400 (`.bad_request`) -- conflating them makes the metric lie.
3. **Per-connection throttling needs the recv stopped**, not a buffer withheld: cancel the multishot recv for that connection and re-arm when drained. Not implemented; it is the remaining piece.

Result: no connection failures at any pipeline depth from 1 to 256, where the previous version lost 31 of 64 connections at depth 64. But throughput stays flat at 44-53k across depths 32-256 while epoll climbs to 198k, because without (3) a busy connection cannot be individually slowed and the shared ring is the only lever.

### Buffer rings

`reactor_uring2` builds its own ring with `.inc = false` rather than using `std`'s `BufferGroup`, which hardcodes incremental consumption. See LESSONS.md — that mode caused one connection to receive another connection's bytes.

---

## http.zig — sans-I/O

```zig
p.feed(bytes) usize;    // bytes consumed; stops at the end of ONE message
p.poll() Event;         // need_input | request | protocol_error
p.reset();
p.isIdle() bool;        // holds nothing
```

No fd, no allocator, no suspension point. A pure function of (state, input), which is why it needs no budget, no cancellation and no effects — all three are driver concerns.

`feed` consuming less than it was given is the normal pipelined case, not an error. **The caller must keep the remainder**; dropping it silently loses the next request.

Because nothing here performs I/O, byte-at-a-time input must produce results identical to whole-message input. `parser_test.zig` asserts that over every split point, plus 20,000 random inputs for the buffer bound. That is the test a fused parser cannot have.

> **Gotcha.** `Request` slices point into the parser's buffer and are valid
> until the next `reset`. The parser must outlive the `Event`. The first
> version of `parser_test.zig` got this wrong and printed garbage.

---

## iobuf.zig — pooled I/O buffers

```zig
p.init(cap);
p.acquire() ?Handle;             // null = pool at the cleanup floor
p.acquireForCleanup() ?Handle;   // may take the last slot; unwinds only
p.release(h);
p.get(h) ?*IoBuf;                // null if the handle is stale
```

Generational handles: a handle for a released buffer fails a check rather than aliasing whoever got the slot next.

Sizing the pool **is** the memory budget, and exhaustion is an admission decision with a real answer (503) rather than an allocation failure.

That took two fixes to actually be true, and it is worth knowing which, because the original measurements were taken before either. `acquire` now stops one slot short of empty and only `acquireForCleanup` may cross that floor, which is the memory version of the cleanup reserve the execution budget already has. One slot is not enough on its own: under a burst every connection fails to acquire in the same reactor pass, so the number of unwinds wanting a buffer at once is the number of connections. When even the reserve is gone the unwind writes its answer straight to the socket from a stack buffer, needing no slot at all.

Before that, a pool of 2 against 48 connections produced 43 `no_buffer` endings and zero delivered 503s. After, 24 connections against a pool of 2 give 3 served and 21 refused with a 503 that arrives. Figures elsewhere in this document that count `no_buffer` are counting refusals the server decided on, not responses a client received.

The CSVs in `results/` corroborate that reading rather than contradicting it. `bufs.csv` and `bufs_e.csv` both report `non2xx=0` on every row, including the rows where the server recorded tens of thousands of refusals. `wrk` counts a reset connection as a socket error and not as a non-2xx response, so a column of zeros beside a large refusal count is the signature of refusals that never reached a client.

Also, unexpectedly, it is a latency knob: fewer buffers means fewer connections with I/O in flight, a shorter queue, and a working set that fits cache (32 x 2904 B = 93 KB vs 3 MB at 1024). p50 1.40 ms -> 168 us with throughput flat and nothing shed.

> **Gotcha.** Release only when the connection truly holds nothing. The guard
> must include `parser.isIdle()` — a partial request lives *only* in the
> parser, and releasing its buffer loses the first fragment of a request split
> across two reads.

> **Gotcha.** The backing store is `undefined` BSS so pages fault in on use.
> Writing `@splat(.{})` over it touches every page and makes RSS the ceiling
> instead of the working set. Three separate bugs in this codebase shared that
> shape: `@splat(null)` over the connection array (13 MB resident at zero
> connections), `register_buffers` over the whole store (23.8 MB pinned), and a
> 512 KB staging buffer sized for a worst case that never happens. **Size
> allocations by the cap, not the ceiling.**

---

## clock.zig

```zig
Clock.real();
Clock.virtualAt(ns);
c.ms(); c.ns();
c.advanceTo(ns); c.advanceBy(d);   // virtual only
```

The scheduler needed no changes to accept this: every time-taking function already took `now` as a parameter. Only the application read a clock.

`tests/sim.zig` drives the **unmodified** scheduler on a virtual clock. Same seed gives a byte-identical trace hash over a million dispatches; different seed, quantum or quota each change it. Six hours of virtual time in 35 seconds of wall clock, because time jumps to the next scheduled event.

---

## Knobs

All are `key=value` on the command line.

| knob | default | effect |
|---|---|---|
| `work_budget` | 1000 | per-request cap |
| `cleanup_reserve` | 50 | reserve for the unwind |
| `quantum_units` | 250 | work charged per turn — a *fairness* knob; only matters when request costs are heterogeneous |
| `idle_deadline_ms` | 3000 | keep-alive timeout |
| `conn_quota` | 0 (unmetered) | aggregate cap for the whole connection class |
| `sup_period_ms` | 100 | supervisor refill period |
| `grant` | 1000 | units per `topUp` |
| `io_bufs` | 1024 | buffer pool size = memory budget = concurrency limiter |
| `bg_budget` / `bg_period_ms` / `bg_quantum` | 400 / 50 / 100 | background task rate limit |
| `tick_ms` | 20 | SIGALRM period |
| `lazy_tick` | 1 | disarm the itimer while blocked |
| `bounded_drain` | 1 | break the drain when the tick fires, so the control surface is seen within one tick |
| `accept_burst` | 512 | accepts per wake |
| `uring_buf_size` | 2048 | bytes per kernel recv buffer |
| `uring_cqe_batch` | 512 | completions reaped per wait |
| `fixed_files` / `fixed_bufs` | 1 / 1 | registered fds and buffers |
| `async_send` | 1 | responses through the ring vs `write(2)` |
| `defer_taskrun` / `coop_taskrun` | 1 / 0 | io_uring setup flags |
| `gen_keys` | 1 | generational completion keys |

---

## Control surface

A dedicated listener on `port+1`, tasks at `prio_ctrl` (above accept), echo-only: no parsing, no budget, no work phase. A control surface that can be made to do arbitrary work is not a control surface.

Priority alone is **not** enough. With an unbounded drain, a keypress waits behind every runnable connection: p50 38.7 ms at 4096 connections, 1300x worse than idle. The fix is to make the tick a deadline for **re-entering the reactor**:

```zig
if (K.bounded_drain != 0 and shared.pending.load(.acquire) != 0) break;
```

That bounds control latency at `tick_period + one quantum`, independent of load. p99 56 ms -> 5.8 ms for 14% throughput. There is a floor around 2.2 ms that more tick does not fix.

> **Gotcha.** `pending` is cleared once per step. An earlier version put this
> check in a second drain loop that ran *after* the block, so a tick that fired
> during the block left `pending` set for the whole loop and capped every pass
> at one task: 50 req/s.

---

## Yield and the stall watchdog

```zig
pub inline fn yieldCheck() bool;   // true = give the turn back
pub inline fn yieldMark() void;    // record progress without being asked
```

The cooperative contract: long-running loops call `yieldCheck()` often and cheaply. Fast path is one load of a usually-zero word and a branch that is usually not taken -- the same load the caller would do anyway to ask "should I stop?". Nothing is incremented unless the tick actually asked.

The tick's job changes accordingly. It stops *enforcing* anything and becomes a **watchdog**: if it fires and the running task has not passed a yield point since the last tick, someone broke the contract.

The property checked is deliberately NOT "did a task switch happen". A long-running task that yields diligently but is never preempted -- because nothing higher priority wanted to run -- produces zero switches and is behaving perfectly. So the *yield* is instrumented, not the dispatch.

`yield_seq` is monotonic and only ever incremented by the mutator; the handler only READS it and compares against handler-private state, so there is no read-modify-write in signal context and no race. The kernel publishes `(dispatch_seq << 20) | task_id` at each dispatch so the watchdog names the culprit rather than just reporting that someone broke the contract.

Escalation is a warning on the first missed tick and a fault after `stall_fault_after` consecutive ones. **Not a kill** -- the seL4 timeout-fault shape: the task is not destroyed, its state is intact, and the supervisor decides.

Measured (5 ms tick, background task with and without the contract):

| | ctrl p50 | ctrl p99 | tick deferral max | warnings | faults |
|---|---|---|---|---|---|
| honours yield | 0.17 ms | 0.26 ms | 22 us | 0 | 0 |
| never yields | 43.60 ms | 50.49 ms | 55,808 us | 281 | 281 |

**2500x on tick deferral, 256x on control p50**, and the watchdog names task 1 as the culprit in the second case. Cost on the fast path: within noise (137-140k req/s with, 140-146k without).

> **Gotcha.** A thread parked in the reactor legitimately passes no yield
> points, so the watchdog must only run while `in_kernel` is false. `lazy_tick`
> also disarms the timer before a real sleep, which masks this -- but do not
> depend on that, or turning `lazy_tick` off makes the watchdog lie.

---

## The tick

```zig
fn onTick(_: posix.SIG) callconv(.c) void {
    if (shared.in_kernel.load(.monotonic)) took_in_kernel += 1
    else took_in_task += 1;
    if (shared.pending.fetchAdd(1, .acq_rel) == 0)
        shared.arrived_ns.store(nowNs(), .monotonic);
}
```

Four atomics and `clock_gettime`. Nothing else in the program has to be async-signal-safe, because `in_kernel` plus a pending **counter** defers all real work to the next kernel entry.

`pending` is a counter, not a flag: under a long quantum many ticks coalesce (34 into one delivery, measured), and a budget scheme has to charge all of them.

> **Gotcha.** `std.atomic.Value(i128)` segfaults in Debug on x86_64 with Zig
> 0.16 — six-line repro, no signals involved, works in ReleaseFast. Timestamps
> here are `i64`. See LESSONS.md.

> **Gotcha.** `lazy_tick` disarms the itimer before a real sleep, because the
> tick bounds *compute* and a thread parked in the reactor is running none.
> Without it an idle server pays 50 wakeups/second forever. With it: 250 -> 5
> wakeups per 5 seconds, and CPU 0.20% -> 0.00%.
>
> But naive lazy-tick made `setitimer` the top non-ring syscall in the
> io_uring build — 69,268 calls — because that build blocks far more often, so
> it disarmed and rearmed on nearly every step. Disarming costs two syscalls;
> leaving the timer armed costs one wakeup per tick period. If you keep waking
> quickly, the pair is the more expensive of the two.
>
> The fix is hysteresis on the **actual** block duration, not the requested
> timeout: under load the requested timeout is still 1000 ms because connection
> deadlines are seconds away, yet the real block is microseconds. Skip the
> disarm when the previous block was shorter than a tick period. Result:
> 69,268 -> **1** `setitimer` call, throughput unchanged, idle behaviour
> unchanged.
