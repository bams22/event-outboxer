# 2026-09-04 (third session) — Jackson vs Protobuf payloads

**Qualifies for README numbers: no.** Same laptop as the other
sessions of the day, standalone PostgreSQL 15 container on the same
host. Compare the variants against each other.

## Question

Does the write format change what an event costs the database, and
does it change throughput? Both formats carry the same three fields
(`seq`, `lockKey`, `padding`); Jackson lands in the JSONB text lane,
Protobuf in the BYTEA binary lane (ADR-0025, ADR-0026). The padding is
random alphanumeric text from a fixed seed, so TOAST compression cannot
hide a size difference and the same bytes go through both formats.

## Setup

As in the [first session](2026-09-04-laptop-first-run.md), harness at
the commit adding `--bench.payload`, in-process fleet. **Backlog mode**
(`--bench.workers-after-publish=true`): the 20 000 events are published
first, then the fleet starts, so the publish rate measures serialize +
insert and the drain rate measures claim + deserialize + finalize,
without the two competing for the host. Payload sizes 256 B, 4 KB and
16 KB; `throughput` preset otherwise (4 types, 3 workers, pool 4, batch
50, no lock key).

```bash
for size in 256 4096 16384; do
  java -jar $JAR $DB --bench.scenario=throughput --bench.workers-after-publish=true \
       --bench.payload=protobuf --bench.payload-bytes=$size
  java -jar $JAR $DB --bench.scenario=throughput --bench.workers-after-publish=true \
       --bench.payload=jackson --bench.payload-bytes=$size
done
```

Two harness changes came out of this session and are in the commit:
the report now carries **WAL bytes per event** (`pg_current_wal_lsn`
sampled before and after) and the **events table size after the
publish phase** (`pg_total_relation_size`), and a run now starts with
**`VACUUM FULL` on the events table**. The second change is the result
of a mistake, described under finding 1.

## Results

Every run passed its invariants. Set 2 is the one to read: table
compacted before every run, Protobuf first at every size.

| Size | Format | publish/s | drain/s | WAL / event | events table after publish |
|---|---|---|---|---|---|
| 256 B | Protobuf | 1 323 | **1 082** | 1.4 KB | 12.0 MB |
| 256 B | Jackson | 1 543 | **1 444** | 1.5 KB | 11.9 MB |
| 4 KB | Protobuf | 1 298 | **1 009** | 5.8 KB | 112.3 MB |
| 4 KB | Jackson | 1 634 | **1 712** | 5.8 KB | 111.9 MB |
| 16 KB | Protobuf | 1 374 | **1 366** | 30.5 KB | 357.7 MB |
| 16 KB | Jackson | 1 364 | **1 982** | 27.0 KB | 355.7 MB |
| 256 B, repeat (Jackson first) | Jackson | 1 967 | 1 381 | 1.5 KB | 12.2 MB |
| 256 B, repeat | Protobuf | 1 568 | 1 142 | 1.4 KB | 11.6 MB |

Row writes per event were `3.00` in every run. Two further pairs at
256 B and 4 KB ran under `pg_stat_statements` and under JFR (below);
they showed the same drain ratio (Protobuf 1 212 vs Jackson 1 361;
Protobuf 1 122 vs Jackson 1 625).

## Findings

### 1. The format does not change what the database pays

Same row writes (`3.00`), same table size to within 1 % after a
compacting vacuum, same WAL to within noise. For this content — random
text that neither encoding can shrink — JSONB and BYTEA cost
PostgreSQL the same. Protobuf can only save what its encoding saves on
the payload itself: numeric fields, repeated structures, enums. A
payload dominated by free text saves nothing.

The WAL column shows a run-order effect that is worth knowing about:
at 16 KB the *second* run of each pair wrote about 10 % less WAL in
both orderings (527 MB vs 595 MB), the first run at a new size
extending the relation and the second reusing it. It is not a format
effect.

**The mistake.** The first six runs of this session were made without
vacuuming between runs, Jackson first at every size. They showed
Protobuf's table 18–23 % *larger* and its drain slower — and the size
part was entirely the previous run's 20 000 dead rows, which autovacuum
had not reclaimed in the thirty seconds between runs. The harness now
runs `VACUUM FULL` on the events table before every run and the sizes
agree. The lesson generalises: any sequence of runs on one database
has an order effect unless the storage is reset, and "second variant
slower" should be the first hypothesis, not the last.

### 2. Serialization CPU is invisible at these rates

JFR with the `profile` settings on the driver JVM during a 4 KB run:
about 320 samples in 30 s, i.e. the JVM was idle almost all of the
time, waiting on the database. Jackson's parser and generator together
took 7 % of the few samples in the Jackson run; Protobuf's UTF-8
length computation and pgjdbc's bytea hex decoding 3 % in the Protobuf
run. The choice of format is not a CPU question for an outbox at
these rates.

### 3. Protobuf drains 20–40 % slower here, and the cause is not in the database

In all eight pairs — either order, every size, with or without
profiling — the Protobuf drain was slower: ratios 0.59–0.89. The
server is not the reason: with `pg_stat_statements` the mean execution
time of every statement matched to the third decimal (claim 0.268 vs
0.277 ms, insert 0.137 vs 0.139 ms, batched finalize deletes 0.96 vs
0.96 ms). CPU is not the reason either (finding 2). The one difference
the profile shows is that pgjdbc decodes the `bytea` column from its
hex text representation (`PGbytea.toBytesHexEscaped`), so the binary
lane travels as text and twice the bytes; at 256 B that cannot
account for milliseconds per event, so it is an observation, not an
explanation. **Open question**, to be answered with a wall-clock
(thread-state) profile of the drain pipeline rather than a CPU one.

### 4. A side observation on the poller, from `pg_stat_statements`

Draining 20 000 events took ~16 500 claim calls returning 1.2 rows
each, in both formats. With the executor saturated, the poller tops
up one event per freed slot (`claim-min-free: 1`), so nearly every
event costs a claim round trip on the poller thread — 0.27 ms of
server time each, plus the network. Group-commit finalize batched
2–2.5 rows per statement under the same conditions. Neither is the
format's doing; both are candidates for a `backlog` session with
`claim-min-free` and `claim-batch-size` varied.

## Console summaries (set 2)

```
protobuf 256B
publish      20000 in 15.1s = 1323/s   p50 6.0ms p95 6.2ms p99 7.0ms max 45.1ms
processing   drained 20000/20000 in 18.5s = 1082/s   e2e p50 15291ms p95 18049ms p99 18804ms max 19185ms
database     60037 row writes (ins 20014, upd 20019, del 20004) = 3.00/event   WAL 28.1MB = 1.4KB/event   events table after publish 12.0MB
jackson 256B
publish      20000 in 13.0s = 1543/s   p50 5.7ms p95 6.2ms p99 6.8ms max 32.1ms
processing   drained 20000/20000 in 13.8s = 1444/s   e2e p50 12768ms p95 13968ms p99 14298ms max 14647ms
database     60023 row writes (ins 20004, upd 20015, del 20004) = 3.00/event   WAL 29.1MB = 1.5KB/event   events table after publish 11.9MB
protobuf 4KB
publish      20000 in 15.4s = 1298/s   p50 6.0ms p95 6.2ms p99 9.0ms max 505.9ms
processing   drained 20000/20000 in 19.8s = 1009/s   e2e p50 16752ms p95 19865ms p99 20260ms max 21025ms
database     60028 row writes (ins 20004, upd 20020, del 20004) = 3.00/event   WAL 114.1MB = 5.8KB/event   events table after publish 112.3MB
jackson 4KB
publish      20000 in 12.2s = 1634/s   p50 4.1ms p95 6.2ms p99 8.9ms max 42.7ms
processing   drained 20000/20000 in 11.7s = 1712/s   e2e p50 11271ms p95 12381ms p99 12496ms max 12607ms
database     60022 row writes (ins 20004, upd 20014, del 20004) = 3.00/event   WAL 113.0MB = 5.8KB/event   events table after publish 111.9MB
protobuf 16KB
publish      20000 in 14.6s = 1374/s   p50 5.4ms p95 8.0ms p99 14.9ms max 48.2ms
processing   drained 20000/20000 in 14.6s = 1366/s   e2e p50 14682ms p95 15424ms p99 15613ms max 15890ms
database     60023 row writes (ins 20004, upd 20015, del 20004) = 3.00/event   WAL 595.1MB = 30.5KB/event   events table after publish 357.7MB
jackson 16KB
publish      20000 in 14.7s = 1364/s   p50 5.9ms p95 8.7ms p99 11.0ms max 93.4ms
processing   drained 20000/20000 in 10.1s = 1982/s   e2e p50 11962ms p95 14763ms p99 14976ms max 15103ms
database     60019 row writes (ins 20004, upd 20011, del 20004) = 3.00/event   WAL 527.1MB = 27.0KB/event   events table after publish 355.7MB
```

Set 1 (no vacuum, Jackson first), `pg_stat_statements` and JFR runs
are kept with the raw JSON reports by the author.

## Next

1. Wall-clock profile of the drain pipeline for the Protobuf case
   (thread states, not CPU samples) — the open question of finding 3.
2. A `backlog` session varying `claim-min-free` and `claim-batch-size`
   for finding 4.
