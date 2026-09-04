# 2026-09-04 (fourth session) — why Protobuf "drained slower": commit latency and a group-commit convoy

**Qualifies for README numbers: no.** Same laptop, standalone
PostgreSQL 15 container on the same host. This session is a root-cause
investigation, not a measurement of the library's speed.

## The question

The [payload-format session](2026-09-04-laptop-payload-formats.md)
left one thing unexplained: in every pair the Protobuf drain was
20–40 % slower than Jackson, while the server executed every statement
in the same time and the JVM was idle. This session finds the cause.

## Method

Bisection with the harness, one factor at a time, always
`throughput` preset in backlog mode, 4 KB payloads, 8 000 events
unless noted, `fsync=on` unless noted; then a wall-clock JFR of the
full configuration with zero thresholds on park, socket-read and
monitor events; then the decisive counter-experiment.

## Results

| Step | Configuration | Protobuf drain/s | Jackson drain/s | Gap |
|---|---|---|---|---|
| serial pipeline | 1 worker, 1 type, pool 1, batch 1, batching off | 167, 144 | 145, 144 | none |
| one type, concurrency | 1 worker, 1 type, pool 4, batch 50 | 369, 323 | 302, 342 | none |
| **four types in one JVM** | 1 worker, 4 types, pool 4 (16 threads) | **284, 299, 347** | **602, 600, 484** | **2×** |
| three JVMs, one type | 3 workers, 1 type, pool 4 | 1 357, 1 347 | 1 236, 977 | none |
| four types, bigger connection pool | as above, pool 32 | 344 | 482 | 1.4× |
| four types, one thread per type | as above, `handler-pool-size 1` | 322 | 343 | none |
| four types, **group commit off**, pool 8 | `finalize-batching=false` | 1 062, 1 067, 1 444, 1 471 | 1 448, 1 380, 1 210, 1 211 | noise |
| four types, group commit off, pool 32 | | 2 269 | 2 280 | none |
| four types, group commit on, **`synchronous_commit=off`** | | 2 913 | 2 906 | none |
| full 3 × 4, 20 000 events, group commit on, `synchronous_commit=off` | | 6 542 | 6 636 | none |

For scale: the same full 3 × 4 configuration with `fsync=on` did
1 000–1 700/s in every earlier session.

**JFR, full 3 × 4 with `fsync=on`, 20 000 events.** Handler threads
parked 19 997 times per run — once per event — in
`GroupCommitEventStore.awaitFlushed`, for 43.5 ms on average with
Protobuf and 31.9 ms with Jackson (869 s vs 637 s of handler-thread
time). Connection-pool waits: 74 and 121 short events. Socket reads on
handler threads (the finalize round trip, commit included): 6.1 ms and
5.3 ms on average. `pg_stat_statements` for the same statements: about
1 ms of execution time. No monitor contention of note.

## Findings

### 1. Every round trip on this host pays a ~5 ms commit

`pg_stat_statements` reports execution time; the commit that follows
an autocommit statement flushes WAL to disk and is not in that figure.
On this laptop's Docker overlay filesystem that flush costs about
4–5 ms, which is exactly the socket-read time the handler threads see
(5–6 ms) against ~1 ms of execution, and exactly the publish p50 of
4–6 ms seen in every session. Turning the flush off
(`synchronous_commit=off`) makes publishing 10× faster and the drain
4–7× faster. **Every number of every laptop session was bound by
commit latency, not by the engine, the locker, or the serializer.**

`synchronous_commit=off` is a diagnostic, not a recommendation: an
outbox exists to be durable. Real database servers with
battery-backed or NVMe storage commit in a fraction of a millisecond;
the laptop overstates this cost by an order of magnitude.

### 2. Group commit serializes what PostgreSQL would have parallelized

`GroupCommitEventStore.awaitFlushed` is:

```java
flushLock.lock();
try {
    while (!entry.result.isDone()) {
        drainAndFlushOnce();
    }
} finally {
    flushLock.unlock();
}
```

Every finalizing thread must take the flush lock, even one whose entry
a previous owner has already flushed — it acquires the lock only to
observe `isDone()`. Under a 5 ms commit the lock owner holds the lock
for the whole round trip; the other fifteen threads of the JVM park.
The lock is not fair: a newly arriving thread whose entry is *not* yet
flushed can barge ahead of parked threads whose entries *are*, start
the next 5 ms flush, and leave them parked for another cycle. The
measured wait — 32–44 ms per event, six to eight flush cycles — is
that convoy. Batches stayed small (2–5 rows per statement in
`pg_stat_statements`) because arrivals are sparse: the poller tops up
one event per freed slot.

Without group commit, sixteen threads commit concurrently; PostgreSQL
groups their WAL flushes itself, and the drain triples (484 → 1 448/s
on a pool of 8, 2 280/s on a pool of 32). With commit latency removed
instead, the convoy disappears and group commit costs nothing
(2 913/s). The ADR-0014 amendment's claim — fewer finalize round trips
— is true; what it does not say is that under commit-bound latency
those fewer round trips form a serial chain of small batches, and
that costs more than it saves.

### 3. The format was never the cause

With group commit off the two formats are indistinguishable (four
runs each, ±20 % noise in both directions). With commit latency off
they are equal to within 1 %. With one thread per type they are equal.
The 20–40 % gap exists only where the convoy exists — and even there
Protobuf is not "slower": the convoy makes throughput hypersensitive
to the timing of arrivals at the lock, and the two deserialization
paths differ slightly in timing. Which micro-difference seeds the
effect (parse time, the bytea hex decode in pgjdbc, GC) was not
isolated and no longer matters: fix the convoy and the seed has
nothing to amplify.

### 4. What this changes in the earlier sessions

- **Throughput numbers** (1 000–1 700/s on 3 × 4 with `fsync=on`) are
  commit-bound laptop numbers; the engine has at least 4× more in it
  on the same code (6 500/s with the flush removed). The "three row
  writes per event" and lock-cost findings are unaffected.
- The **payload-format** finding 3 is answered by this session.
- The **hot-key** analysis is unaffected in mechanism, but its
  absolute numbers carry the same commit-latency floor.

## Proposed fix (for an ADR-0014 amendment)

Waiters should wait on their **future**, not on the lock:

1. Enqueue the entry.
2. `tryLock()`: the winner drains and flushes in a loop **until the
   queues are empty**, completing every drained future, then releases.
3. A thread that did not get the lock does `entry.result.join()`.
4. Race guard: after the owner releases, a thread whose entry is still
   not done (enqueued between the owner's last drain and its release)
   takes the lock and flushes — the current loop, but only for that
   case.

Threads whose work was done by the owner then never touch the lock,
the convoy cannot form, and the owner's loop keeps batches as large as
the arrival rate allows. Validation: the `1 worker × 4 types` matrix
above with `fsync=on`, group commit on, expecting drain rates at the
group-commit-off level or better and equal across formats.

A second, independent knob is worth measuring in the same session:
`claim-min-free` above 1, so the poller tops up several events per
round trip and arrivals at the flush lock bunch naturally.

## Next

1. ADR-0014 amendment with the fix above; re-run this session's matrix
   before and after.
2. Redo the throughput and payload sessions once, after the fix, on a
   machine whose commit latency is representative — or at least with
   the `synchronous_commit=off` figures reported side by side as the
   engine's own ceiling.
