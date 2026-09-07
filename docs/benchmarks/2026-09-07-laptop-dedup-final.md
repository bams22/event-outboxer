# 2026-09-07 (twelfth session) — claim-time coalescing as shipped: the ADR-0037 implementation against the spike and the insert-time design

**Qualifies for README numbers: no.** Same laptop as the
[first session](2026-09-04-laptop-first-run.md); PostgreSQL 15.17 in a
standalone container on the same host, recreated at the start of the
session. Read the cells against each other.

## Question

The [spike session](2026-09-05-laptop-dedup-claim-time.md) measured
claim-time coalescing behind a flag and decided ADR-0037. The
implementation that shipped (`3c17416`, `b4d9a61`) differs from the
spike in what the claim statement returns — the dedup key and, per
representative, the ids and stored carriers of the swept duplicates,
so that the dispatcher can fire `onEventCoalesced` and link the
consumer span — and in everything around it (listener, tracing, admin,
in-memory adapter). Did the shipped code keep the spike's numbers, and
where does it stand against the insert-time design it replaced?

## Setup

| | |
|---|---|
| Host, JVM, PostgreSQL | as in the spike session: Intel Core i7-13700H, Temurin 25.0.2, `postgres:15` with `shared_buffers=1GB`, `max_connections=200`, `fsync=on`, `pg_stat_statements` preloaded; `--bench.jdbc-url`, one container recreated for the session |
| Library | `b4d9a61` (ADR-0037 implemented, index folded into V004) for the first matrix; the A/B below compares `b4d9a61` with the working tree of this session (see §The RETURNING shape) |
| Harness | the same `dedup-burst` preset and `--bench.dedup-keys`; `--bench.dedup` is gone with the flag |
| Fleet, method | in-process, 3 workers; one run per cell for the matrix, then an interleaved A/B with 3 + 2 + 2 pairs; `VACUUM FULL` at every run start; nothing else running on the host |

```bash
JAR=event-outboxer-benchmark/target/event-outboxer-benchmark-*-exec.jar
DB="--bench.jdbc-url=jdbc:postgresql://127.0.0.1:55432/bench --bench.jdbc-user=bench --bench.jdbc-password=bench"
java -jar $JAR $DB --bench.scenario=throughput
for k in 8 64 1024; do java -jar $JAR $DB --bench.scenario=dedup-burst --bench.dedup-keys=$k; done
java -jar $JAR $DB --bench.scenario=dedup-burst --bench.dedup-keys=64 --bench.workers-after-publish=true
```

JSON reports and console logs:
`outbox/bench-reports/2026-09-07-laptop-dedup-final/` (outside this
repository); the spike's are under `.../2026-09-05-laptop-dedup/`.

## Results

Every cell of every run `PASS`: no lost event, no duplicate, no stale
key, storage clean. "insert" is the pre-ADR-0037 design measured on
2026-09-05, "spike" the flagged claim-time statement of the same day,
"shipped" this session's first matrix on `b4d9a61`.

### `throughput`, no dedup key

| | publish/s | publish p95 | e2e p99 | row writes/event | statements/event | claim mean |
|---|---|---|---|---|---|---|
| insert | 1 474 | 6.2 ms | 131 ms | 3.00 | 1.38 | 0.77 ms |
| spike | 1 447 | 6.2 ms | 115 ms | 3.00 | 1.37 | 0.77 ms |
| shipped | 1 483 | 6.2 ms | 126 ms | 3.00 | 1.37 | 0.87 ms |

### `dedup-burst`, steady state

| keys | design | publish/s | publish p95 / p99 | handler runs | coalesced | e2e p50 / p99 | writes / publish | statements / publish | claim mean |
|---|---|---|---|---|---|---|---|---|---|
| 8 | insert | 1 352 | 6.8 / 12.0 ms | 302 | 98.5 % | 259 / **1 970 ms** | 0.05 | 2.02 | 0.27 ms |
| 8 | spike | 1 327 | 6.2 / 6.6 ms | 3 146 | 84.3 % | 56 / 116 ms | 2.16 | 1.10 | 2.00 ms |
| 8 | shipped | 1 454 | 6.2 / 6.6 ms | 2 782 | 86.1 % | 50 / **129 ms** | 2.14 | 1.09 | 2.62 ms |
| 64 | insert | **1 074** | 12.0 / 12.4 ms | 18 159 | 9.2 % | 359 / 400 ms | 2.73 | 2.03 | 0.47 ms |
| 64 | spike | 1 602 | 6.1 / 6.4 ms | 15 494 | 22.5 % | 294 / 366 ms | 2.78 | 1.73 | 0.47 ms |
| 64 | shipped | **1 448** | 6.2 / 6.7 ms | 14 369 | **28.2 %** | 306 / 392 ms | 2.72 | 1.64 | 0.69 ms |
| 1 024 | insert | 1 212 | 9.3 / 12.1 ms | 17 580 | 12.1 % | 1 083 / 1 374 ms | 2.64 | 2.06 | 0.42 ms |
| 1 024 | spike | 1 313 | 6.2 / 6.6 ms | 15 904 | 20.5 % | 887 / 1 325 ms | 2.80 | 1.77 | 0.50 ms |
| 1 024 | shipped | 1 481 | 6.2 / 6.8 ms | 15 904 | 20.5 % | 823 / 1 224 ms | 2.80 | 1.73 | 1.86 ms |

### `dedup-burst`, backlog (20 000 published on 64 keys, then the fleet starts)

| design | handler runs | coalesced | drain | writes / publish | WAL / publish | claim calls × rows, mean |
|---|---|---|---|---|---|---|
| insert | 64 | 99.7 % | 321 ms | 0.01 | 107 B | 7 × 9.1, 0.29 ms |
| spike | 240 | 98.8 % | 613 ms | 2.01 | 984 B | 22 × 10.9, 5.09 ms |
| shipped | 468 | 97.7 % | 1.2 s | 2.02 | 995 B | 24 × 19.5, 11.25 ms |
| shipped, rerun | 259 | 98.7 % | 661 ms | 2.01 | 982 B | 24 × 10.8, 5.32 ms |

## The RETURNING shape: a regression that was the host

The first matrix showed the claim statement's server time above the
spike in every cell (0.87 vs 0.77 ms without a key, 1.86 vs 0.50 ms at
1 024 keys, 11.25 vs 5.09 ms in the backlog cell). The one thing the
shipped statement adds is its `RETURNING`: `dedup_key` plus two
correlated `array_agg` subqueries over the `gone` CTE. `EXPLAIN
ANALYZE` on a seeded backlog (3 000 due rows over 1 024 keys, batch
50) confirmed that those subqueries run once per returned row —
`loops=50`, ~1.6 ms of the statement — so a variant that aggregates
`gone` once per key in a `swept` CTE and `LEFT JOIN`s it was built and
measured, first by `EXPLAIN ANALYZE` (medians ~2.8 ms against ~3.4 ms
for the subqueries and ~2.4–3.5 ms for the spike's shape, with 2×
noise between runs), then by an interleaved A/B on the harness,
alternating the two jars cell by cell:

| cell | shape | claim mean per run | median | publish/s median | e2e p99 median |
|---|---|---|---|---|---|
| `throughput` | subqueries (HEAD) | 1.68 / 0.82 / 0.87 ms | 0.87 ms | 1 451 | 114 ms |
| `throughput` | aggregated | 1.31 / 0.79 / 0.80 ms | 0.80 ms | 1 445 | 138 ms |
| 1 024 keys | subqueries (HEAD) | 0.69 / 1.23 ms | 0.96 ms | 1 417 | 1 420 ms |
| 1 024 keys | aggregated | 0.74 / 1.41 ms | 1.07 ms | 1 528 | 1 190 ms |
| 64 keys | subqueries (HEAD) | 0.51 / 0.56 ms | 0.54 ms | 1 438 | 388 ms |
| 64 keys | aggregated | 0.53 / 0.54 ms | 0.54 ms | 1 385 | 1 159 ms* |

\* one of the two aggregated 64-key runs had an e2e p99 of 1.9 s with
an otherwise identical profile; the other 382 ms. Same jar, same cell,
consecutive runs.

The two shapes are indistinguishable within the noise of this host:
consecutive runs of the *same* jar differ by 2× (1.68 vs 0.82 ms at
`throughput`, 0.69 vs 1.23 ms at 1 024 keys), and the backlog rerun of
the shipped code landed exactly on the spike (5.32 vs 5.09 ms, 259 vs
240 runs). The first matrix's excess was the laptop's commit regime,
not the subqueries. The aggregated variant ships as the claim statement's
shape — one pass over the swept rows instead of two scans per
returned row, which matters exactly where the sweep is large — on the
strength of the plan, not of a measured win: on this host the two
shapes are equal.

## Findings

1. **The shipped implementation holds the spike's numbers.** Publish
   p95 6.2 ms in every keyed cell, one statement instead of two per
   keyed publish (1.09–1.73 vs 2.02–2.06), coalescing 86 % / 28 % /
   20.5 % at 8 / 64 / 1 024 keys against the spike's 84 % / 22.5 % /
   20.5 %, the same row writes and WAL per publish, the keyless path at
   3.00 writes and ~0.8 ms per claim. The extra `RETURNING` and the
   listener and tracing work per swept duplicate cost nothing the
   harness can see.
2. **Against the insert-time design the gains of the spike are
   intact.** Under the 8-key burst the end-to-end p99 is 129 ms where
   the pinned row starved for 1.97 s; at 64 keys publishers no longer
   serialize (p95 12.0 → 6.2 ms, publish rate 1 074 → 1 448/s); where
   duplicates are scattered the claim collapses three times as many of
   them (9.2 → 28.2 % at 64 keys).
3. **The costs are the spike's costs.** Dense duplicates pay two row
   writes and ~1 KB of WAL each (0.05 → 2.14 writes per publish at 8
   keys, 0.01 → 2.02 in the backlog cell) where the insert-time design
   paid a conflicting no-op, and a hot-key burst runs the handler some
   nine times more often (302 → 2 782), each run fresh. Neither moved
   between spike and shipped code.
4. **Single runs on this host do not resolve claim-statement
   differences below ~2×.** Any future claim-shape decision needs the
   interleaved A/B, not a matrix.

## Follow-ups

- Bloat over a long run with a dense key is still unmeasured (the
  spike session's open item).
- A `pg_stat_statements` mean per *returned row*, next to the mean per
  call, would make claim-shape comparisons less sensitive to batch
  size; the harness reports only the per-call mean today.
