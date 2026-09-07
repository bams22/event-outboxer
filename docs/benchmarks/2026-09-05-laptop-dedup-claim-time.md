# 2026-09-05 (eleventh session) — dedup coalescing: insert-time pin versus claim-time sweep

**Qualifies for README numbers: no.** Same laptop as the
[first session](2026-09-04-laptop-first-run.md); PostgreSQL in a
standalone container on the same host. Read the cells against each
other.

## Question

ADR-0021 coalesces duplicates of a `(event_type, dedup_key)` at
publish time: a conditional insert against the V004 partial unique
index, and on conflict a `SELECT ... FOR UPDATE` *pin* of the existing
PENDING row until the publishing transaction commits, so that the
row's handler cannot start before the coalesced transaction is
visible. A design review of the self-rescheduling ("recurring event")
pattern found a defect in that mechanism: the unique index covers
PENDING rows only, a twin may be inserted while the key's event is
PROCESSING (by design, otherwise the lost-update race of ADR-0021
returns), and every transition of the PROCESSING row *back* to PENDING
— `markForRetry`, `release` for a busy lock, `releaseClaimed` on
shutdown, `forceReclaim`, `reclaimOrphans`, `reenable` — then collides
with the index (`23505`), leaves the row stuck in PROCESSING and, for
`reclaimOrphans`, fails the whole recovery batch of a dead worker.

The proposed alternative moves coalescing to the claim: every keyed
publish inserts unconditionally, and the claim statement collapses the
*due* duplicates of the keys it picked before any handler runs. Under
READ COMMITTED only committed rows are visible to that sweep, so every
swept publish had committed before the representative's handler
started — the ADR-0021 visibility guarantee without a pin, without a
unique index, and therefore without the collision.

Two questions for the harness before any ADR: does the heavier claim
statement cost anything, and what does the design pay and gain where
duplicates are dense and where they are sparse?

## Setup

| | |
|---|---|
| Host | Intel Core i7-13700H (20 logical CPUs), Linux 7.0, Temurin 25.0.2 |
| PostgreSQL | 15.17, `postgres:15` in a standalone container on the same host, `shared_buffers=1GB`, `max_connections=200`, `fsync=on`, `shared_preload_libraries=pg_stat_statements`; `--bench.jdbc-url`, one container for the session |
| Library | working tree over `1a78101`: the claim-time spike behind `event-outboxer.storage.dedup-mode=claim` (`DedupMode.CLAIM` in the PostgreSQL adapter), default `insert` unchanged |
| Harness | the same working tree: `dedup-burst` preset, `--bench.dedup-keys`, `--bench.dedup`, the per-key freshness invariant, the `pin` statement class and the mean server-side time of the claim statement from `pg_stat_statements` |
| Fleet | in-process, 3 workers |
| Method | one run per cell, no warm-up; `VACUUM FULL` at every run start; the harness swaps the V004 unique index for a plain index in `claim` mode and restores it in `insert` mode |

```bash
docker run -d --name outboxer-bench-pg -e POSTGRES_USER=bench -e POSTGRES_PASSWORD=bench \
  -e POSTGRES_DB=bench -p 127.0.0.1:55432:5432 postgres:15 \
  -c shared_buffers=1GB -c max_connections=200 -c shared_preload_libraries=pg_stat_statements
JAR=event-outboxer-benchmark/target/event-outboxer-benchmark-*-exec.jar
DB="--bench.jdbc-url=jdbc:postgresql://127.0.0.1:55432/bench --bench.jdbc-user=bench --bench.jdbc-password=bench"
for m in insert claim; do java -jar $JAR $DB --bench.scenario=throughput --bench.dedup=$m; done
for k in 8 64 1024; do for m in insert claim; do
  java -jar $JAR $DB --bench.scenario=dedup-burst --bench.dedup-keys=$k --bench.dedup=$m; done; done
for m in insert claim; do
  java -jar $JAR $DB --bench.scenario=dedup-burst --bench.dedup-keys=64 --bench.dedup=$m --bench.workers-after-publish=true; done
```

`dedup-burst`: 20 000 publishes from 8 threads on `dedup-keys` keys,
one event type, 3 workers × pool 4, claim batch 50, 2 ms of simulated
work (so that rows are PROCESSING long enough for twins to arrive
mid-handling — the case the two designs treat differently). Under
dedup keys the drain is graded by the events table (empty = done), and
the per-sequence `lost` rule is replaced by the per-key **freshness**
rule: every key's last publish must be followed by a successful
handling of that key that started after that publish committed.
"Handler runs" below is the number of distinct publishes that reached
a handler; "coalesced" is the rest. JSON reports and console logs:
`outbox/bench-reports/2026-09-05-laptop-dedup/` (outside this repository).

## Results

All ten cells `PASS`: no lost event, no duplicate, no stale key,
storage clean.

### `throughput`, no dedup key (20 000 events, 4 types, batch 50)

| mode | publish/s | publish p95 | handled/s | e2e p99 | row writes/event | statements/event | claim mean |
|---|---|---|---|---|---|---|---|
| insert | 1 474 | 6.2 ms | 1 469 | 131 ms | 3.00 | 1.38 | 0.77 ms |
| claim | 1 447 | 6.2 ms | 1 440 | 115 ms | 3.00 | 1.37 | 0.77 ms |

### `dedup-burst`, steady state (workers running while 8 threads publish)

| keys | mode | publish/s | publish p95 / p99 | handler runs | coalesced | e2e p50 / p99 | row writes / publish | WAL / publish | statements / publish | pin calls | claim calls × rows, mean |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 8 | insert | 1 352 | 6.8 / 12.0 ms | 302 | 98.5 % | 259 / **1 970 ms** | 0.05 | 122 B | 2.02 | 19 851 | 226 × 1.3, 0.27 ms |
| 8 | claim | 1 327 | 6.2 / 6.6 ms | 3 146 | 84.3 % | 56 / **116 ms** | 2.16 | 1.0 KB | 1.10 | 0 | 418 × 7.5, **2.00 ms** |
| 64 | insert | **1 074** | 12.0 / 12.4 ms | 18 159 | 9.2 % | 359 / 400 ms | 2.73 | 1.4 KB | 2.03 | 2 990 | 7 353 × 2.5, 0.47 ms |
| 64 | claim | **1 602** | 6.1 / 6.4 ms | 15 494 | 22.5 % | 294 / 366 ms | 2.78 | 1.4 KB | 1.73 | 0 | 6 744 × 2.3, 0.47 ms |
| 1 024 | insert | 1 212 | 9.3 / 12.1 ms | 17 580 | 12.1 % | 1 083 / 1 374 ms | 2.64 | 1.4 KB | 2.06 | 3 116 | 8 520 × 2.1, 0.42 ms |
| 1 024 | claim | 1 313 | 6.2 / 6.6 ms | 15 904 | 20.5 % | 887 / 1 325 ms | 2.80 | 1.4 KB | 1.77 | 0 | 7 456 × 2.1, 0.50 ms |

### `dedup-burst`, backlog (all 20 000 published on 64 keys, then the fleet starts)

| mode | events table after publish | handler runs | coalesced | drain | row writes / publish | WAL / publish | statements / publish | claim calls × rows, mean |
|---|---|---|---|---|---|---|---|---|
| insert | 64 rows, 768 KB | 64 | 99.7 % | 321 ms | 0.01 | 107 B | 2.00 | 7 × 9.1, 0.29 ms |
| claim | 20 000 rows, 12.2 MB | 240 | 98.8 % | 613 ms | 2.01 | 984 B | 1.01 | 22 × 10.9, **5.09 ms** |

## Findings

### 1. The claim statement costs nothing without duplicates and ~5 µs per swept duplicate

Without a key the two statements are indistinguishable: 0.77 ms mean
in both modes, 3.00 row writes per event, throughput within noise
(1 469 vs 1 440/s). With keys and few duplicates per claim they are
still equal (0.42–0.50 ms at 64 and 1 024 keys). The statement grows
only with what it sweeps: 2.0 ms when every claim removes some 48
duplicates (8 keys: 20 004 deletes over 418 claims), 5.1 ms when it
removes some 900 (backlog: 20 004 over 22 claims, the 1 000-row cap in
effect) — about 5 µs per swept row, which is a plain index probe and a
heap delete riding the claim's commit. No extra commit is paid
anywhere: the sweep commits with the claim, the duplicate's insert
commits with the business transaction, as the conflicting insert did
before.

### 2. The pin is the expensive half of the current design

Insert-time coalescing pays a second round trip for every coalesced
publish — the `pin` column equals the coalesced count plus the retries
— so a keyed publish costs 2.0 statements where the claim-time design
costs 1.0–1.8. Worse, the pin and the unique index serialize
publishers of one key on each other's commits: publish p95 is 12.0 ms
at 64 keys and p99 12.0 ms at 8 keys (two commits of ~6 ms on this
host), while every claim-mode cell holds p95 at 6.1–6.2 ms, one
commit. At 64 keys that is a 49 % higher publish rate (1 074 →
1 602/s) and a 27 % faster drain.

### 3. Under a burst on few keys the pin starves the representative

With 8 publishers on 8 keys the PENDING row of a key is pinned by
*someone* almost all the time, and the claim (`SKIP LOCKED`) passes it
by until a gap appears: insert mode ran the handler 302 times for
20 000 publishes with an end-to-end p99 of 1.97 s and a maximum of
2.08 s. Claim mode ran it 3 146 times with a p99 of 116 ms. Both
respect the guarantee; they sit at opposite ends of a trade the
harness cannot grade — fewer handler runs against fresher ones.
Publishers do not queue behind each other in claim mode, so the
representative is claimable at every poll.

### 4. Where duplicates are scattered, the sweep coalesces twice as much

At 64 and 1 024 keys claim mode collapsed 22.5 % and 20.5 % of the
publishes against 9.2 % and 12.1 % for insert mode, and drained with
fewer handler runs. Part of the gap has a precise cause in the pin
protocol: the pin found its row already claimed 1 149 times at 64 keys
and 696 times at 1 024 (pin calls minus coalesced publishes), and the
publisher then fell back to a row of its own — the "vanished" branch
of ADR-0021 — which the claim-time design never needs, since whatever
is due at the next claim collapses together.

### 5. The price is rows and WAL where duplicates are dense

Every duplicate is now an insert plus a delete instead of a conflicting
no-op: 2.16 against 0.05 row writes and 1.0 KB against 122 B of WAL per
publish at 8 keys; 2.01 against 0.01 and 984 B against 107 B in the
backlog cell, where the table held 20 000 rows (12.2 MB) instead of 64
when the fleet started. Where duplicates are few the two designs are
within 2–6 % of each other (2.78 vs 2.73, 2.80 vs 2.64). In other
words the claim-time design bills a duplicate like an ordinary keyless
event — two row writes, ~1 KB of WAL — while the insert-time design
bills it a pin and a wait. Dead tuples and vacuum pressure follow the
row writes; this session did not measure bloat over time.

## What the spike does not yet do

The spike is the two mechanics that the numbers depend on — the
unconditional insert and the sweeping claim statement — behind a flag,
plus the harness. Left for the real change: the SPI (`EventStore.save`
back to a plain insert, `lockPendingByDedupKey` gone, `saveAll`
accepting keyed events, the claim reporting the swept ids), the
listener and tracing contract (`onEventCoalesced` and
`coalesced_into` move from the publisher to the claim side; `publish`
always returns its own id), the archive decision (the spike archives
swept rows as `coalesced at claim` when archiving is on), the
in-memory adapter, migration V009 replacing the unique index with a
plain one, and the retirement of the flag.

## Decision proposed

Write the ADR that supersedes ADR-0021 with claim-time coalescing and
amends ADR-0033 (the replay arbiter is no longer needed). The
measurable case for it: no cost on the keyless path, one statement
instead of two per keyed publish, no serialization of publishers, an
order of magnitude shorter tail under a hot-key burst, more coalescing
where duplicates are scattered, and the `23505` collision gone by
construction. The measurable case against it: two row writes and a
kilobyte of WAL per duplicate where the insert-time design paid a
no-op, and a table that holds the burst until the next claim. Whether
a user with a very hot key prefers 302 handler runs with a two-second
tail or 3 146 runs with a tenth of a second is a product decision the
ADR has to state; it is not a defect of either design.

## Follow-ups

- The in-flight sweep is capped at 1 000 rows per claim; the backlog
  cell reached the cap (909 per claim) at 5 ms per statement. Whether
  the cap should be a property or scale with the claim batch is an ADR
  question.
- A regression test for the collision itself (twin published while
  PROCESSING, then retry / release / reclaim / reenable) belongs in the
  contract suite regardless of which design ships.
- Bloat over a long run with a dense key was not measured; a
  `dedup-burst` cell of several minutes with `pg_stat_user_tables`
  dead-tuple counts would settle it.
