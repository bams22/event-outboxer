# Benchmark runs

Recorded runs of the [benchmark and invariant harness](../modules/event-outboxer-benchmark.md)
(ADR-0034). One file per session, newest first. Each file states the
hardware, the database, the harness commit and the exact commands, and
carries the harness's own console summary verbatim, so a number never
travels without its configuration.

| Date | Setup | What it answers |
|---|---|---|
| [2026-09-04, bounded lock wait](2026-09-04-laptop-lock-wait.md) | same laptop, plus Redis 7 | the ADR-0035 validation matrix that fixed `lock-wait` at 100 ms: on 5 ms holds the wait removes every busy round trip (lease 6.65 → 5.04 writes/event, Redis 5.38 → 3.01) and raises drain rate by a third; invisible without a key; the feared mixed-workload regression turned out the other way (a 200 ms hot key drains 2.3× faster, the cool keys pay 0.6 s of median); after a crash the wait falls back to the release path; the one loser is an uncapped virtual executor on a hot key, where polling waiters cost more than they save |
| [2026-09-04, platform vs virtual executor](2026-09-04-laptop-executors.md) | same laptop | at equal concurrency the two executors are equal in throughput and database cost and differ only in platform threads (2 022 vs 45 for 2 000 blocking handlers); flipping the switch on an unchanged configuration raises executing concurrency 4 → 104 per type, which multiplies blocking-handler throughput and feeds group commit whole claims (finalize batches 40–120 rows, ~1.05 statements/event) — and multiplies busy hits on hot keys ten-fold (237/s → 96/s, 6.6 → 21 row writes/event) |
| [2026-09-04, publish-only sweeper](2026-09-04-laptop-publish-only-sweeper.md) | same laptop | a correctness defect found through the target seam: a publish-only instance derived a zero stale-claim threshold and reset every in-flight claim of the fleet each sweep — 390 duplicates on the `crash` preset, 0 after the ADR-0029 amendment |
| [2026-09-04, group commit after the fix](2026-09-04-laptop-group-commit-after-fix.md) | same laptop | before/after the convoy-free flush path (ADR-0014 amendment): batching on goes from 318 to 1 662/s in the convoy cell, batches 9–10 rows, a third fewer statements everywhere; wins on commit-bound storage with ordinary pools, loses where every thread can commit in parallel; default stays `true` |
| [2026-09-04, group commit + Redis locker](2026-09-04-laptop-group-commit-redis.md) | same laptop, plus Redis 7 | the same on/off matrix with the Redis locker in the path: on unique keys under fsync batching now wins by 20–60 % (8–9-row batches, 1.6 vs 2.4 statements per event) because the locker's round trips jitter the arrivals; in one JVM without fsync it still loses; on hot keys it does not matter |
| [2026-09-04, group-commit matrix](2026-09-04-laptop-group-commit-matrix.md) | same laptop | `finalize-batching` on vs off × pool size × fleet shape × commit regime, with `pg_stat_statements` counts: group commit lost in 15 of 16 cells (1.5–6.4×), spends on claims what it saves on finalizes; `finalize-batching: false` recommended until the flush path is fixed |
| [2026-09-04, group-commit convoy](2026-09-04-laptop-group-commit-convoy.md) | same laptop | root cause of the Protobuf "gap": every round trip pays a ~5 ms commit on this host, and group commit's flush lock turns concurrent commits into a convoy of small batches; with commit latency removed the engine does 6 500/s and both formats are equal. Fix proposed for ADR-0014 |
| [2026-09-04, payload formats](2026-09-04-laptop-payload-formats.md) | same laptop | Jackson vs Protobuf at 256 B / 4 KB / 16 KB: identical database cost (rows, WAL, disk) for text payloads, serialization CPU invisible, Protobuf drain 20–40 % slower for a reason not yet found; a run-order (bloat) mistake caught and fixed with `VACUUM FULL` at run start |
| [2026-09-04, locks](2026-09-04-laptop-locks.md) | same laptop, plus a standalone Redis 7 container | lease vs Redis locker: two extra row writes per locked event on PostgreSQL with the lease, none with Redis (4 Redis commands instead); Redis faster under contention; the busy-hit accounting corrected |
| [2026-09-04](2026-09-04-laptop-first-run.md) | developer laptop, standalone PostgreSQL 15 container on the same host | first numbers: `throughput` and `hot-key` presets, six control variants; the hot-key path under a locker is the first "looks bad" scene |

## How to read a run

- **Invariants** (`lost`, unexplained `duplicates`, `unexpected`,
  `lockOverlaps` when graded, `storage clean`) are the verdict. A run
  with a failed invariant is a bug report, whatever its numbers say.
- **Numbers** (publish/s, handled/s, end-to-end percentiles, row writes
  per event) are information about *that* host, database and
  configuration. Compare them across variants of the same session, not
  across sessions on different hardware.
- **Row writes per event** is the most portable figure: it depends on
  the engine's design, not on the host. `3.00` is the design minimum
  (insert, claim, finalize-delete).

## Policy (ADR-0034 §8)

Numbers quoted in the README or in module docs must come from a run
against an external PostgreSQL on separate hardware, state that
hardware, and include the `hot-key` scenario with the locker on. A
session recorded here does not automatically qualify: the file says
whether it does.
