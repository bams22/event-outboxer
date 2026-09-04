# 2026-09-04 (sixth session) — group commit with the Redis locker in the path

**Qualifies for README numbers: no.** Same laptop; standalone
PostgreSQL 15 and Redis 7 containers on the same host.

## Question

The [group-commit matrix](2026-09-04-laptop-group-commit-matrix.md)
was measured without an entity locker. With a locker the lock is held
through finalize, so the flush-lock convoy also lengthens every key's
hold time. Does group commit help or hurt once the Redis locker is in
the path — on unique keys (the cost of locking, no contention) and on
hot keys (contention)?

## Setup

As in the previous session (backlog mode, `pg_stat_statements`,
regimes A `fsync=on` and B `synchronous_commit=off`), plus
`--bench.lock=redis` against a standalone `redis:7-alpine`. Unique
keys: `throughput` shape, 10 000 events of 256 B,
`--bench.lock-keys=100000`. Hot keys: the `hot-key` preset (5 000
events on 8 keys, 5 ms of work, 3 workers × 1 type, pool 3). The
statement classifier learned this session to tell the adapter's
`UPDATE` shapes apart by their SET lists (`pg_stat_statements`
normalises literals), so `release` and `retry` are now their own
classes.

## Results

Drain rate, events/s; every run passed its invariants, no lock key
left behind.

| Configuration (Redis locker) | A: fsync, batching **on** | A: fsync, **off** | B: no fsync, **on** | B: no fsync, **off** |
|---|---|---|---|---|
| unique keys, 1 worker × 4 types, pool 8 | **1 478** (repeat 1 257) | 1 202 (repeat 1 059) | 4 521 | **7 370** |
| unique keys, 3 workers × 4 types, pool 8 | **3 889** | 2 465 | **6 412** | 4 478 |
| hot keys, 3 × 1, pool 3 | 307, 289, 326 | 338, 381, 319 | 385 | 330 |
| *control, no locker, 1 × 4, pool 8 (same session)* | 367 | **1 387** | — | — |

Statements per event (regime A, unique keys, 1 × 4):

| | batching on | batching off |
|---|---|---|
| calls per event | 1.60–1.63 | 2.34–2.39 |
| claim calls (rows per call) | 4 735–4 983 (2.0–2.1) | 3 431–3 899 (2.6–2.9) |
| finalize | 1 141–1 245 batched at **8.0–8.7 rows**, 20–116 single | 10 000 single |
| Redis commands per event | 4.00 | 4.00 |

Hot keys, regime A: 4.0 statements per event either way, of which
**5 389–5 784 are lock-busy releases** (1.1 per event), 5.1–5.2 Redis
commands per event; batched finalizes carried 2.0 rows.

## Findings

### 1. With the locker, group commit flips from harmful to helpful — at commit-bound latency

Unique keys under fsync: batching on beats off by 23 % (repeat: 19 %)
in one JVM and by 58 % in three. Statements per event drop from 2.4 to
1.6, and the batched finalize carries 8–9 rows against a cap of 128 —
three times the 2.9 rows seen without a locker in the same session.

Without fsync the picture splits: +43 % with three JVMs, but −39 % in
one (4 521 vs 7 370/s). When a round trip is cheap, the flush lock's
serialization costs more than the statements it saves in a single JVM.

### 2. Why the locker changes the outcome: it jitters the arrivals

The Redis locker adds two round trips per event on the handler thread
(`SET NX PX` before the handler, `EVAL` after finalize), each a few
hundred microseconds. That is enough to spread the sixteen threads'
arrivals at the flush lock: the lock owner finds eight or nine queued
entries instead of three, and the poller tops up two rows per claim
instead of one. The same-session control without a locker shows the
lockstep case: batches of 2.9 rows, claims of 1.2, and batching on
losing 3.8× (367 vs 1 387/s).

So the feature's value is decided by arrival timing — the convoy's
signature from the [fourth session](2026-09-04-laptop-group-commit-convoy.md).
A locker's jitter happening to help is not something to design for;
the convoy-free flush path proposed there would make the batches
depend on the arrival *rate*, not on its phase.

### 3. On hot keys it does not matter

Six runs across both regimes: 289–385/s with batching on, 319–381/s
off — noise. The hot-key path is dominated by the busy cycle (1.1
lock-busy releases per event, four statements per event) that
[ADR-0035](../adr/0035-bounded-lock-wait.md) addresses; finalize is
not where its time goes.

### 4. The recommendation gets a second clause

- Handlers **without** lock keys, or on **hot** keys:
  `finalize-batching: false` (previous session; unchanged).
- Handlers **with** keys on a locker, against commit-bound storage:
  `true` is better today by 20–60 % — for a reason that the flush-path
  fix will remove. Measure after the fix before keeping either
  setting.

Both clauses belong in the ADR-0014 amendment together with the fix.

## Console lines (regime A, unique keys, 1 × 4, pool 8)

```
batching on
processing   drained 10000/10000 in 6.8s = 1478/s   handlings=10000 retries=0
statements   16250 calls = 1.63/event   claim 4983 calls x 2.0 rows   finalize batched 1141 calls x 8.7 rows, single 116   release 0   other 10
redis        40007 commands = 4.00/event   lock keys left=0
batching off
processing   drained 10000/10000 in 8.3s = 1202/s   handlings=10000 retries=0
statements   23441 calls = 2.34/event   claim 3431 calls x 2.9 rows   finalize batched 0 calls x 0.0 rows, single 10000   release 0   other 10
redis        40007 commands = 4.00/event   lock keys left=0

hot keys, batching on / off (classifier with release class)
processing   drained 5000/5000 in 15.3s = 326/s   handlings=5000 retries=0
statements   20076 calls = 4.02/event   claim 4850 calls x 2.2 rows   finalize batched 579 calls x 2.0 rows, single 3841   release 5784   retry 0   other 22
processing   drained 5000/5000 in 15.7s = 319/s   handlings=5000 retries=0
statements   19940 calls = 3.99/event   claim 4528 calls x 2.3 rows   finalize batched 0 calls x 0.0 rows, single 5000   release 5389   retry 0   other 23
```

The full set of runs and JSON reports is kept by the author with the
raw data.
