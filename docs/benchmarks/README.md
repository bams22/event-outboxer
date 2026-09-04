# Benchmark runs

Recorded runs of the [benchmark and invariant harness](../modules/event-outboxer-benchmark.md)
(ADR-0034). One file per session, newest first. Each file states the
hardware, the database, the harness commit and the exact commands, and
carries the harness's own console summary verbatim, so a number never
travels without its configuration.

| Date | Setup | What it answers |
|---|---|---|
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
