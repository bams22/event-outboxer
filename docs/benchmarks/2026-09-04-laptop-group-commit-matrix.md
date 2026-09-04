# 2026-09-04 (fifth session) — what group commit contributes

**Qualifies for README numbers: no.** Same laptop, standalone
PostgreSQL 15 container on the same host. Compare the cells against
each other.

## Question

The ADR-0014 amendment introduced group-commit finalize batching
("up to ~batch-size× fewer finalize round-trips"). The
[previous session](2026-09-04-laptop-group-commit-convoy.md) found a
convoy on its flush lock under commit-bound latency. This session
measures the feature's net contribution: the same configurations with
`finalize-batching` on and off, in two commit-latency regimes, with
the database statement counts to show *why*.

## Setup

As before; `throughput` preset shape (pool 4 per type, claim batch 50),
backlog mode, 10 000 events of 256 B, one run per cell. The container
preloads `pg_stat_statements`, and the harness now reads it: the
report's `statements` line is calls and rows per statement class
(claim, insert, batched finalize, single finalize, release, other)
sampled before and after the run. Regime A is the default
(`synchronous_commit=on`, `fsync=on`, ~5 ms per commit on this host);
regime B is `synchronous_commit=off`, a stand-in for a server whose
commits cost a fraction of a millisecond.

```bash
docker run ... postgres:15 -c shared_preload_libraries=pg_stat_statements -c pg_stat_statements.track=all [-c synchronous_commit=off]
java -jar $JAR $DB --bench.scenario=throughput --bench.workers-after-publish=true --bench.events=10000 \
     --bench.payload-bytes=256 --bench.handler-pool-size=4 --bench.claim-batch-size=50 \
     --bench.workers=<1|3> --bench.event-types=4 --bench.connection-pool-size=<8|32> \
     [--bench.finalize-batching=false] [--bench.worker-prop.event-outboxer.event-types.defaults.claim-min-free=25]
```

## Results

Drain rate, events/s. Every run passed its invariants.

| Configuration | A: fsync, batching **on** | A: fsync, batching **off** | B: no fsync, **on** | B: no fsync, **off** |
|---|---|---|---|---|
| 1 worker × 4 types, pool 8 | 318 | **1 390** | 2 679 | **10 681** |
| 1 worker × 4 types, pool 32 | 365 | **2 326** | 2 758 | **10 911** |
| 3 workers × 4 types, pool 8 | 1 626 | **3 564** | 6 225 | 6 219 |
| 1 × 4, pool 8, `claim-min-free=25` | 499 | **1 525** | 2 739 | **8 152** |

Statement counts (regime A, 1 × 4, pool 8; the other cells look alike):

| | batching on | batching off |
|---|---|---|
| calls per event | 2.37 | 2.21 |
| claim calls (rows per call) | 8 361 (1.2) | 2 130 (4.7) |
| finalize calls | 2 374 batched (3.0 rows) + 2 910 single | 10 000 single |
| with `claim-min-free=25`: calls per event | 1.67 | 2.04 |

## Findings

### 1. Group commit never helped, in any of the sixteen cells

Batching on was slower in fifteen cells, by 1.5× to 6.4×, and equal
in one (three JVMs, no fsync, where something else caps both at
~6 200/s). The worst cases are the shape the feature was designed for
— many handler threads in one JVM finalizing concurrently.

### 2. The round trips it saves on finalize, it spends on claims

With batching on, 10 000 finalizes became 5 284 statements (2 374
batched at 3.0 rows, 2 910 single) — a saving of 4 716 round trips.
But the claim side went from 2 130 calls at 4.7 rows to 8 361 calls at
1.2 rows: 6 231 more. Net: *more* statements per event with batching
on (2.37 vs 2.21). The mechanism is the coupling with capacity-aware
polling: threads parked in the flush convoy keep the executor
saturated, so the poller tops up one freed slot at a time
(`claim-min-free: 1`), and every event costs a claim round trip of
its own.

`claim-min-free=25` breaks that coupling (415 claims at 24 rows) and
is the most statement-efficient cell of the matrix at 1.67 calls per
event — yet batching on still loses to batching off (499 vs 1 525/s),
because the convoy itself remains.

### 3. It is the lock, not the fsync

Removing commit latency (regime B) lifts everything, but batching on
is still 4× slower than off in a single JVM (2 679 vs 10 681/s). The
flush lock serializes finalizes whatever the round trip costs; fsync
only makes each turn of the convoy longer. The mechanism and the fix
are in the previous session's report: waiters should join their own
future, and the lock owner should drain until the queues are empty.

### 4. Batches stay small because arrivals are sparse

Batched finalizes carried 2.6–3.2 rows in every cell, against a cap of
128. With one event topped up per freed slot, finalizations never
bunch. Larger claims (`claim-min-free`) bunch the arrivals but the
convoy then queues them one flush at a time.

## What to do with this

- **Until the flush path is fixed, `event-outboxer.dispatcher.finalize-batching: false`
  is the better setting in every configuration measured here.** The
  default is `true`; changing it is a decision for an ADR-0014
  amendment, together with the fix.
- After the fix, re-run exactly this matrix. The feature's intended
  benefit (fewer statements at high concurrency) should then show in
  regime B first — that is where round trips are cheap and the lock
  was the only cost.
- `claim-min-free` is a real knob: 25 cut statements per event by a
  third here, at the price of waiting for 25 free slots before a
  top-up, which cost throughput when handlers were very fast (8 152 vs
  10 681/s with batching off, no fsync). Worth its own session after
  the fix, not a blanket recommendation.

## Console lines (regime A, 1 × 4, pool 8)

```
batching on
processing   drained 10000/10000 in 31.4s = 318/s   handlings=10000 retries=0
statements   23667 calls = 2.37/event   claim 8361 calls x 1.2 rows   finalize batched 2374 calls x 3.0 rows, single 2910   release 0   other 22
batching off
processing   drained 10000/10000 in 7.2s = 1390/s   handlings=10000 retries=0
statements   22140 calls = 2.21/event   claim 2130 calls x 4.7 rows   finalize batched 0 calls x 0.0 rows, single 10000   release 0   other 10
```

The full set of 16 console summaries and JSON reports is kept by the
author with the raw data.
