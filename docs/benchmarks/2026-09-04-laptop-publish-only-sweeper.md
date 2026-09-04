# 2026-09-04 (eighth session) — the publish-only sweeper, found through the target seam

**Qualifies for README numbers: no.** A correctness finding, not a
measurement.

## How it surfaced

ADR-0034 §2 lets a team measure another outbox implementation with
the same scenarios by implementing the three target types outside this
repository. Preparing such a comparison, the `crash` preset was run
against event-outboxer with the forked fleet at 5 000 events — larger
than the 600-event `BenchmarkCrashIT` — and failed with
**390 duplicates, 186 of them nowhere near the kill**, with the lease
locker; 75 (32 unexplained) with the Redis locker.

## Bisection

| Variant | duplicates (unexplained) |
|---|---|
| crash, lease, as shipped | 331 (298), 65 (63), 112 (78), 390 (186) across four runs |
| `dead-threshold 30s` (a live worker cannot be mistaken for dead) | 210 (182) |
| `handler-max-runtime 5m` (watchdog and derived stale threshold out of the picture) | 156 (128), drain timed out behind 10-minute leases |
| **`stale-claim-threshold 20s` set explicitly in every context** | **0** |

The ledger dump (now written to the work directory whenever a run
fails) showed every unexplained duplicate as: first handled by a
*live* worker on attempt 1, then handled again on attempt 2 seconds
later. Only three code paths bump `attempts`: orphan reclaim,
stuck-handler reclaim, stale-claim sweep. The harness's workers now
register an `OutboxListener` that logs each of them; it stayed silent
in the workers — and spoke in the **driver's publish-only context**:

```
[bench-publisher] stale-swept count=299 threshold=PT0S
[bench-publisher] stale-swept count=249 threshold=PT0S
[bench-publisher] stale-swept count=45 threshold=PT0S
```

## The defect

The stale-claim threshold, when not configured, is derived as 2 × the
largest `handler-max-runtime` of the types an instance polls. A
publish-only instance (ADR-0029) polls none; the maximum over nothing
is zero; a zero threshold means *every* `PROCESSING` row of the fleet
is returned to `PENDING` with `attempts + 1` on every sweep. Rows
sitting in live workers' executor queues were then handled twice —
once by the original claimer from its in-memory queue (whose finalize
lost the version race), once by whoever re-claimed the row. In
production, with the default 5-minute sweep cadence, a publish-only API
tier would have done this to every in-flight event every five minutes.

Fixed the same day (ADR-0029 amendment): an instance that polls no type
runs the sweeper only with an explicit `maintenance.stale-claim-threshold`,
and logs that at startup. After the fix, the same cells:

| Cell (fixed core) | duplicates (attributable / unexplained) | verdict |
|---|---|---|
| crash, Redis locker | 4 (4 / 0) | PASS |
| crash, lease locker | 2 (2 / 0) | PASS |
| pg-restart, Redis locker | 0 | PASS (was 44 "attributable" — the sweeper's, inside the ±10 s window) |

## What the harness learned

- `BenchmarkTarget` names its own tables (`eventsTable()`,
  `leaseTable()`), so the vacuum, size and cleanliness checks work for
  any target.
- A failed run dumps the full ledger as CSV into the work directory.
- Every context the target boots — the publish-only one included —
  carries a listener that logs orphan reclaims, stale sweeps,
  stuck-handler reclaims, abandoned handlers, retries and storage
  errors with the worker id. Without it the reclaim that caused the
  duplicates was invisible: the engine reports these only through the
  listener.
- `BenchmarkCrashIT` at 600 events passed all along. A correctness
  invariant that holds at one size is not evidence at another; the
  preset's 5 000 events are the size to grade at.
