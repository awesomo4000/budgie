# The story

How this got built, what went wrong, and what it taught. The bugs are the
interesting part; the code is mostly a record of having found them.

Written from one session. Every number quoted was measured on a **single
shared vCPU** with the load generator on the same core.

---

## 1. It started as a code review

The session opened by analysing an unrelated Zig document-conversion project.
Three findings from that review turned out to matter later, because the same
mistakes recurred in our own code:

- Its XML parser handed out slices into a buffer that the next call
  invalidated, with nothing in the type saying so.
- Its `docx` reader silently degraded on a malformed `styles.xml` — headings
  became plain paragraphs with no diagnostic.
- Its correctness story was "the reader author wrote a report for what they
  knew they skipped". Nothing in the test suite would catch a reader that was
  confidently *wrong* rather than admittedly incomplete.

All three reappeared here in different clothes.

---

## 2. A tick, then a kernel

The first artifact was ~400 lines: a cooperative kernel with an `in_kernel`
flag, a SIGALRM tick, and two tasks (a counter and a primality test) so the
work per quantum grew over time.

**First real bug, and a good one.** `std.atomic.Value(i128)` segfaults:

```
Segmentation fault at address 0x8
/opt/zig/lib/std/atomic.zig:25:34: in store
```

Six-line repro, no signal handler involved. Debug lowers a 128-bit atomic store
to `lock cmpxchg16b` through a null double-indirection; ReleaseFast lowers the
same store to one `vmovdqa` and works; `-mcpu=baseline` refuses to compile it.
Two open upstream issues (13989, 14235) at `urgent` for three and a half years.

I initially explained it as "a compiler-rt libcall that isn't
async-signal-safe". That was wrong, and I only found out by disassembling. The
correct explanation is a Debug-mode codegen bug, and the async-signal-safety
concern is real but separate.

**Lesson: a plausible explanation that fits the symptom is not a diagnosis.**
This became a theme.

---

## 3. The server, and the first big win

Then a real server: connections as state machines, budgets, a cleanup reserve,
deadlines as the `poll` timeout rather than a watchdog.

First measurements: throughput flat at ~20k req/s, latency perfectly linear in
connection count. The server's own counters gave it away immediately:

```
conns=1     steps=266369  polls=266369
conns=2048  steps=202861  polls=202861
```

`steps == polls`, exactly, always. The loop did one `poll()` and then ran
**one** connection's quantum. Fixing it — drain everything runnable per poll —
gave **5.9x throughput and 5.8x latency**.

**Lesson: instrument the loop, not just the endpoints.** The throughput number
said "slow". The step counter said exactly why.

---

## 4. Separating scheduler from reactor

Everything was fused in one `step()`. Splitting into `sched.zig` (no fds) and
`reactor.zig` (no budgets) gave another **4x at 1024 connections**, because
both O(max_tasks) scans became O(ready).

A pleasant discovery fell out: `sched.zig` reads no clock. Every time-taking
function already took `now` as a parameter, which made the deterministic
simulator a 62-line `clock.zig` and zero scheduler changes.

The separation held for the rest of the session. **`sched.zig` was
byte-identical across four reactor ports** — poll, epoll, io_uring readiness,
io_uring completion.

---

## 5. Things that were built and mostly just worked

Not everything was a disaster. These landed cleanly:

- **Timer wheel** replacing a heap. `heap_stale=500532` in one run — half a
  million dead entries pushed and later discarded. The wheel creates none.
  Modest throughput gain, but `arm` became O(1) and the "slack window" idea it
  was supposed to need turned out to be unnecessary.
- **Two-currency accounting.** Units enforce, nanoseconds observe. It
  immediately caught me lying: the "work" loops were being constant-folded
  (`sum of j` has a closed form), so every "900 units of work" measurement to
  that point had measured scheduler round-trips, not CPU.
- **Supervisors.** Budget as an object tasks draw from, with `topUp` deducting
  from every ancestor. Aggregate throttling of 256 connections from one number,
  with p50 *dropping* as the quota tightened — admission control rather than
  queueing.
- **Control surface.** Priority alone was not enough (p50 38.7 ms at 4096
  connections); making the tick a deadline for re-entering the reactor took p99
  from 56 ms to 5.8 ms for 14% throughput.
- **Deterministic simulation.** Same seed, byte-identical trace hash over a
  million dispatches. Six hours of virtual time in 35 seconds.

---

## 6. Memory: the same bug three times

RSS was 13.1 MB and completely insensitive to load — identical at zero
connections and at 4096. The cause was one line:

```zig
conns: [max_tasks]?Conn = @splat(null),
```

`?Conn` is ~1.4 KB, and the optional tags are 1.4 KB apart, so `@splat(null)`
touches **every page** of an 11.5 MB array at startup. Splitting hot metadata
from the payload and leaving the payload `undefined` in BSS took idle RSS from
13 MB to **1.85 MB**, and made it scale with use.

Later, the same mistake twice more:

- `register_buffers` on the *entire* 8192-slot store — 23.8 MB **pinned** for a
  pool that might hand out 32 slots. Registration must match the cap, not the
  ceiling.
- A 512 KB staging buffer sized for a worst case that never occurred.

**Lesson: size allocations by the runtime cap, not the `max_*` constant.**
Worth a lint. It is the single most repeated error in this codebase.

---

## 7. Background work, and being wrong about priority

The background task got a refilling budget, and the rate limiter was exact:
`bg_iters` tracked `bg_budget` linearly with zero drift at every scale, with
one starvation per period, every period.

Then a genuinely surprising result. Comparing where to put background work:

| placement | throughput | p99 | bg iterations |
|---|---|---|---|
| none | 129,317 | 3.30 ms | 0 |
| in-thread, **priority only** | 45,394 | 8.08 ms | 302,444 |
| in-thread, priority + budget | 120,797 | 4.64 ms | ~8,500 |
| separate process, `SCHED_IDLE` | 125,701 | 3.83 ms | 348,973 |

**Priority alone made it three times worse.** An always-runnable idle task
means `anyRunnable()` is permanently true, so the reactor timeout is always 0
and **the thread never blocks** — 229,437 waits instead of 4,497, time asleep
2.3% instead of 80%.

I had said "priority decides who runs, budget decides how long". True but
incomplete. In a single-threaded cooperative runtime, **budget is the only
mechanism that makes a task stop being runnable, and being not-runnable is the
only way the thread can sleep.** Priority is purely internal; the OS cannot see
it. Only a separate schedulable entity can express "run when the machine has
slack".

---

## 8. io_uring: four hours of confident wrong answers

This is the part worth reading.

The POLL_ADD port was a true drop-in and gave +10%, with `strace` showing
108,098 syscalls collapsing to 1,175 — a **92x** reduction that bought almost
nothing, because a syscall on this box costs **74 ns** (no KPTI, no Spectre
mitigations; typical production x86 is 5-20x more).

Then the completion port: multishot recv, provided buffer ring, registered
files, registered buffers, async sends. Architecturally correct, every feature
applied, `read` and `write` gone from the syscall profile entirely — and **3x
slower than epoll**.

I diagnosed it, confidently and incorrectly, five times:

1. *Missing `DEFER_TASKRUN`.* Added it. `cqes/enter` stayed at 1.03.
2. *The per-wait timeout SQE.* Replaced with a periodic one. No change.
3. *Not waiting for a batch.* `submit_and_wait(N)` — collapsed to 545 req/s,
   because a timeout produces one completion and the call kept blocking for
   real ones that never came.
4. *The async send path.* Reverted to plain `write`. No change.
5. *Inherent to completion-per-arrival on one core.* Wrote a long explanation
   about epoll's batching being "a symptom of falling behind".

Explanation 5 was elegant, internally consistent, and wrong.

**What actually found it: a 40-line trace ring.** Record every mutation of the
inbound state, dump the last 64 on failure:

```
acquire   t=4  buf=1
onData+   t=4  used=264   0->264   plen=0
drainIn   t=4  used=17  264->247   plen=17     <- fresh parser consumed 17, not 33
```

Task 4's bytes started 16 into a request. A few lines above, task 3 had
received a stray 16-byte chunk — **task 4's prefix, delivered to task 3.**

The cause: `std.os.linux.IoUring.BufferGroup` hardcodes `.inc = true`,
incremental buffer consumption, where several completions share one buffer id
with a head offset maintained in userspace by `put`. Batching the `get`s and
deferring the `put`s made later slices use a stale head. Replacing it with a
hand-built ring using `.inc = false` — one whole buffer per completion — fixed
it instantly.

With that fixed:

| | throughput | p50 | p99 | client ctxsw |
|---|---|---|---|---|
| epoll | 97,067 | 2.30 ms | 5.55 ms | 54,906 |
| io_uring | 126,121 | 1.92 ms | 3.28 ms | **2,314** |

**io_uring wins on all three**, and the client's context switches drop from
106,000 to 2,314 — the per-request ping-pong gone entirely. Confirmed
independently with `wrk`: 119,449 vs 113,274.

**Lesson: five plausible performance explanations were worth less than one
trace ring.** When the same class of hypothesis fails twice, stop hypothesising
and record state.

**Second lesson, worse:** that bug delivered one connection's bytes to another
connection's parser. In a session-carrying system that is cross-tenant data
leakage. It presented as a *throughput anomaly* and was invisible at pipeline
depth 1.

---

## 9. The load generator was the bottleneck

Partway through, the measurements stopped meaning anything, and it took too
long to notice. `bench.zig` had two defects:

1. It counted responses by scanning for `"HTTP/1.1"` and then **discarded the
   read buffer**, so any response split across two reads desynchronised the
   outstanding-request accounting.
2. It issued one `write()` per request with `TCP_NODELAY`, so a depth-32
   pipeline went out as 32 segments and arrived as 32 separate events. **The
   server never saw a batch because the client never sent one.**

`pipelined_kept` was 27 KB across 268,000 requests — the smoking gun.

The fix was `gen.zig`: byte-exact `Content-Length` framing carrying partials
across reads, pipelined bursts in a single write, io_uring on the client side,
and its own context-switch reporting. It found four more server bugs within
minutes of existing.

**`wrk` was in the package repositories the whole time.** One `apt-get`. I
should have reached for it hours earlier instead of trusting a load generator I
had written in ten minutes.

**Lesson: validate the instrument before trusting the measurement.** A
benchmark result is a claim about two programs, not one.

---

## 10. Claiming bugs that were not bugs

Twice I "fixed" something and called it a real bug when it was not.

The clearest case: I rewrote `http.zig`'s `feed()` to stop at the message
boundary, and wrote that the old `header_len - before` arithmetic was
"a genuine bug". Later testing of the old implementation verbatim:

```
A  single request per feed : used=33 (correct = 33)
B  8-deep burst, 1st feed  : used=33 (correct = 33)  parser_len=264
C  remainder, fresh parser : used=33 (correct = 33)
```

Correct in every case. And `pipetest.zig` had already passed with the old code
before I touched it. The rewrite is still *better* — it bounds `p.len` to one
header instead of copying 264 unrelated bytes — but it is a cleanup, not a fix.

**Lesson: "this expression looks suspicious and the symptom is nearby" is not
evidence.** Test the old code before declaring it broken.

---

## 11. Disk, and what io_uring is actually for

Late on, the right benchmark. O_DIRECT random 4K reads, so real device latency
and no page cache to hide behind:

```
pread, 1 in flight            22,038 IOPS   45.4 us    1.00x
io_uring, 1 in flight         26,217 IOPS   38.1 us    1.19x
io_uring, 4 in flight        104,194 IOPS    9.6 us    4.73x
io_uring, 64 in flight       341,864 IOPS    2.9 us   15.51x
io_uring, 128 in flight      367,778 IOPS    2.7 us   16.69x
```

**16.7x.** At depth 1 it is 1.19x — the same marginal syscall win as everywhere
else. The entire advantage is **queue depth**, which a serial `pread` loop
structurally cannot have.

Earlier I had benchmarked page-cached reads and reported 1.16x, concluding
io_uring's storage advantage was overstated. That was the wrong test: with no
device latency there is nothing to overlap, so it measured `memcpy`.

And it settles the priorities. One 4K disk read is 45 us. One HTTP request here
costs **2.3 us of CPU**. A single disk touch is worth ~20 requests of compute,
so once real storage is in the path the network layer stops being the
bottleneck — and the reactor choice stops being a coin flip and becomes an
order of magnitude.

---

## 12. The buffer pool turned out to be a latency knob

Sweeping `io_bufs` with wrk, expecting a memory/throughput tradeoff:

```
io_bufs    tput      p50      p99    non-2xx
      4  111,159    26 us    54 us      0
     32  128,845   168 us   307 us      0
     64  128,687   1.38 ms  2.22 ms     0
   1024  126,760   1.40 ms  2.20 ms     0
```

**p50 falls 8x with throughput flat and zero requests shed.** The pool doubles
as a concurrency limiter: fewer buffers means fewer connections with I/O in
flight, a shorter queue, and a working set that fits cache (93 KB vs 3 MB).

epoll cannot do this — the same sweep starves it to 7k req/s, because its
buffers are effectively per-connection. It is a lever the completion
architecture gives you and the readiness one does not.

The default of 1024 was leaving an 8x latency win on the table, and nothing
about the code suggested it.

---

## 13. And then the box drifted

Near the end, a change appeared to cost 22% throughput. I A/B'd it with a
runtime flag: identical. Then re-measured *unchanged* epoll code that had read
119-128k earlier in the session:

```
85,065 req/s
84,543 req/s
```

Everything was down ~30% because the VM had been running benchmarks for hours.

**Lesson: absolute numbers are only comparable within the same few minutes.**
Every relative result in this document was paired A/B for that reason; every
absolute number should be re-measured before being quoted.

---

## Lessons, condensed

**On debugging**

1. A plausible explanation that fits the symptom is not a diagnosis. I was
   wrong five times consecutively about io_uring and twice about "bugs" that
   were not.
2. When the same *class* of hypothesis fails twice, stop hypothesising and
   record state. A 40-line trace ring beat four hours of reasoning.
3. Instrument the loop, not the endpoints. `steps == polls` found in seconds
   what throughput numbers could not express.
4. Validate the instrument before trusting the measurement.
5. Absolute performance numbers decay. Pair every comparison.

**On design**

6. Size allocations by the runtime cap, not the `max_*` ceiling. Three separate
   bugs, one shape.
7. Admission control must fail closed. An unmetered default grants infinity to
   exactly the task nobody configured.
8. Put the structural check in one place. Per-phase cancellation checking is
   the same mistake as per-call-site `catch`.
9. Generational handles everywhere an identity can be recycled: pool slots,
   cancel tokens, **completion keys**. Two of this session's worst bugs were
   stale identities delivering data to the wrong owner.
10. Sans-I/O protocol code is testable in ways fused code is not. The parser
    proved clean in eight lines of output while the integration around it was
    broken.
11. Separation earns its keep when you swap something. Four reactors, one
    unchanged scheduler.
12. In a cooperative runtime, budget — not priority — is what lets the process
    sleep.

**On performance**

13. Measure what the thing is *for*. io_uring on page-cached reads: 1.16x. On
    real device I/O with queue depth: 16.7x.
14. The unaccounted fraction is the most useful number in the profile. 96% of
    wall time charging no units says budget tightening will not find your next
    win.
15. Correctness machinery was free. Two-currency accounting, budgets,
    supervisors, generational keys — all within noise. There was no tradeoff to
    make.
