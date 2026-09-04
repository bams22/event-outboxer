# 2026-09-04 (seventh session) — group commit after the convoy-free flush path

**Qualifies for README numbers: no.** Same laptop, same standalone
containers. Before/after on the same code base within one day; every
"before" cell comes from the [fifth](2026-09-04-laptop-group-commit-matrix.md)
and [sixth](2026-09-04-laptop-group-commit-redis.md) sessions.

## What changed

`GroupCommitEventStore.awaitFlushed` no longer blocks on the flush
lock. Waiters wait on their own future; the lock is taken with
`tryLock` only; an owner flushes once per acquisition and, if it leaves
work behind, wakes the waiters through a hand-off generation so one of
them takes over. Design, liveness sketch and the decision on the
default are in the
[ADR-0014 amendment of 2026-09-04](../adr/0014-optimistic-locking-via-version-field.md#convoy-free-flush-path-amendment-2026-09-04).

## Results

Drain rate, events/s; 10 000 events of 256 B, backlog mode, pool 4 per
type, claim batch 50, `pg_stat_statements` on. Every run passed its
invariants.

| Configuration | before: on | before: off | after: on | after: off | after, on vs off |
|---|---|---|---|---|---|
| fsync, 1 × 4, pool 8 | 318 | 1 390 | **1 662** | 1 342 | +24 % |
| fsync, 1 × 4, pool 32 | 365 | 2 326 | 1 981 | **3 012** | −34 % |
| fsync, 3 × 4, pool 8 | 1 626 | 3 564 | **4 836** | 3 187 | +52 % |
| fsync, 1 × 4, pool 8, `claim-min-free 25` | 499 | 1 525 | 1 648 | 1 687 | −2 % |
| fsync, Redis, unique keys, 1 × 4 | 1 478 | 1 202 | 1 293 | 1 285 | +1 % |
| fsync, Redis, unique keys, 3 × 4 | 3 889 | 2 465 | **4 172** | 2 472 | +69 % |
| no fsync, 1 × 4, pool 8 | 2 679 | 10 681 | 5 537 | **10 270** | −46 % |
| no fsync, 1 × 4, pool 32 | 2 758 | 10 911 | 5 185 | **6 909** | −25 % |
| no fsync, 3 × 4, pool 8 | 6 225 | 6 219 | 6 004 | 6 146 | −2 % |
| no fsync, Redis, unique keys, 1 × 4 | 4 521 | 7 370 | 3 292 | **4 835** | −32 % |
| no fsync, Redis, unique keys, 3 × 4 | 6 412 | 4 478 | 3 721 | **4 294** | −13 % |

Statements per event and batch shape (fsync, 1 × 4, pool 8):

| | before: on | after: on | off |
|---|---|---|---|
| calls per event | 2.37 | **1.58** | 2.22 |
| claim calls (rows per call) | 8 361 (1.2) | 4 719 (2.1) | 2 177 (4.6) |
| batched finalize (rows per call) | 2 374 (3.0) | 1 080 (**9.3**) | — |
| single finalize | 2 910 | **1** | 10 000 |

Across all "after, on" cells: batched finalizes 7.7–10.2 rows,
single finalizes 0–39 (280 in one Redis cell), 1.46–1.82 statements
per event.

## Findings

### 1. The convoy is gone

Batching on went from 318 to 1 662/s in the cell that exposed the
convoy, with batches of nine rows instead of three and no single-row
flushes at all. The claim side recovered with it: 4 719 claims at 2.1
rows instead of 8 361 at 1.2, because handler threads no longer sit in
the convoy keeping the executor saturated.

### 2. What group commit is, once it works: statements for parallelism

One flusher per JVM issues one statement per round trip. Where a round
trip is expensive and connections are scarce, that is the right trade:
+24 % in one JVM and +52 % in three on a pool of 8 under fsync, +69 %
with the Redis locker in three JVMs, and a third fewer statements. Where
commits are cheap (no fsync) or the pool lets every thread commit
concurrently (pool 32), sixteen single-row statements in flight beat one
nine-row statement at a time; PostgreSQL groups their WAL flushes
itself. The feature cannot win there by design — its ceiling is
`rows per batch / round trip` per JVM.

### 3. The locker no longer decides the outcome

In the sixth session the Redis locker's round trips were what made
batching win, by jittering the arrivals. After the fix the no-locker
and Redis cells move together (+24 % / +1 % in one JVM, +52 % / +69 %
in three under fsync). Batches depend on the arrival rate now, not on
its phase.

### 4. Recommendation

The default stays `true`. The two-clause advice of sessions five and
six ("`false` until the fix") is superseded: keep batching on for
commit-bound storage and ordinary pools, where it reduces round trips
by a third and is neutral or better on throughput; turn it off only
for a single JVM that has to sustain several thousand finalizes per
second on sub-millisecond storage, or when a pool sized for fully
parallel commits is cheaper than the statements saved.

## Next

1. The `hot-key` path is unchanged by this fix (ADR-0035 is the
   proposal there); its numbers were not re-run.
2. `claim-min-free` remains a two-sided knob; with the convoy gone the
   default of 1 no longer costs a claim per event under batching (2.1
   rows per claim here), so its session can wait.
