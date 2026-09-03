# ADR-0033: Archive dedup key + replay from archive

## Status

Accepted

## Date

2026-09-03

## Context

Two operational gaps around the opt-in archive (ADR-0008):

- **The dedup key dies at archive time.** The `event_archive` table has
  no `dedup_key` column — ADR-0021 deliberately noted "the key's
  meaning ends when the row leaves PENDING". That is true for the
  *coalescing mechanics*, but it also erases audit data: when
  investigating "why did the sync run once, not three times", the
  archive cannot say which key the executions coalesced under.
- **Re-execution requires hand-written SQL.** Operators sometimes need
  to run an already-processed event again (a handler shipped with a bug
  between 10:00 and 12:00; events "completed" but the work was wrong).
  At-least-once semantics make re-execution legal by contract — handler
  idempotency is already mandatory (ADR-0015) — but the library offered
  no supported path from `event_archive` back to `events`.

## Decision

### 1. The archive carries `dedup_key` (migration V008)

`ALTER TABLE event_archive ADD COLUMN dedup_key VARCHAR(256)` —
nullable, **no index, no uniqueness**. Both archiving CTEs
(`markProcessed`, `markProcessedAll`) copy the column; `ArchivedEvent`
gains a trailing `dedupKey` component. Runtime coalescing semantics are
untouched: the partial unique index stays on the hot `events` table
only, and archive rows never block a key. Rows with the same
`(event_type, dedup_key)` accumulate in the archive naturally — the key
is reusable after every processed run. Rows archived before V008 keep
`NULL`.

### 2. Replay from archive on the `OutboxAdmin` port

Two new methods (ADR-0019 surface):

- `replayFromArchive(id)` → `ReplayOutcome {REPLAYED, COALESCED,
  ID_IN_USE, NOT_FOUND}`
- `replayAllFromArchive(eventType, archivedAfter?, archivedBefore?,
  limit, after?)` → `ReplayAllResult(replayed, coalesced, idInUse,
  next)` — window bounds exclusive, rows taken oldest-archived first,
  `after`/`next` a keyset `ArchiveCursor(archivedAt, id)`.

Because both window bounds are exclusive, an `archivedAfter` that is not
strictly before `archivedBefore` cannot match a row. That is rejected
with `IllegalArgumentException` rather than answered with zeroed counts:
a UI with its two date pickers swapped would otherwise report "nothing
to replay" for an incident window, and the operator would read that as
"already done".

A replayed row re-enters `events` as a fresh operator-initiated event:
`status='PENDING'`, `attempts=0`, `version=0`, `created_at=now()`,
`run_at=now()`, `last_fail_reason='replayed from archive'`; `priority`,
payload lanes, `trace_context` and `dedup_key` are copied verbatim.
Pollers pick it up on the next cycle — no engine involvement.

`created_at` is deliberately *not* preserved. It is the column the whole
operational surface ages rows by — `purgeDisabled` (retention has no
"disabled at" column, ADR-0019), `reenableAll(type, createdBefore, …)`
and `findByStatus`'s `(created_at, id) DESC` keyset. Carrying a
year-old publish time back into the hot table would make the replay
immediately eligible for the next retention sweep — the operator would
lose a failed replay before being able to inspect it — and would bury it
on the last admin page. The replay is a new delivery lifecycle, so it
gets a new clock; the original publish time remains readable in the
archive right up to the moment the row is moved.

### 3. Single-statement, insert-first CTE

```sql
WITH src AS MATERIALIZED (
    SELECT ..., archived_at FROM event_archive WHERE ...
), blocked AS (
    SELECT s.id FROM src s WHERE EXISTS (SELECT 1 FROM events e WHERE e.id = s.id)
), ins AS (
    INSERT INTO events (..., attempts, status, created_at, run_at,
                        last_fail_reason, version, dedup_key)
    SELECT ..., 0, 'PENDING', now(), now(), 'replayed from archive', 0, dedup_key FROM src
    WHERE id NOT IN (SELECT id FROM blocked)
    ON CONFLICT (event_type, dedup_key)
        WHERE status = 'PENDING' AND dedup_key IS NOT NULL DO NOTHING
    RETURNING id
), del AS (
    DELETE FROM event_archive a USING ins WHERE a.id = ins.id RETURNING a.id
)
SELECT (SELECT count(*) FROM src)     AS found,
       (SELECT count(*) FROM ins)     AS inserted,
       (SELECT count(*) FROM blocked) AS id_in_use,
       (SELECT archived_at FROM src ORDER BY archived_at DESC, id DESC LIMIT 1)
                                      AS cursor_archived_at,
       (SELECT id FROM src ORDER BY archived_at DESC, id DESC LIMIT 1) AS cursor_id
```

- **Coalesce keeps the archive row.** The `ON CONFLICT` arbiter is the
  same clause the publisher's insert uses (V004): if a `PENDING` event
  with the same `(event_type, dedup_key)` already exists, the work is
  already scheduled — nothing is inserted, and because `del` deletes
  only ids the INSERT actually returned, the audit row survives
  *structurally*, not by statement ordering luck.
- **Duplicate keys within one bulk batch** resolve the same way: the
  second row's speculative insert conflicts against the first's and is
  skipped by `DO NOTHING`; `ORDER BY archived_at, id` in `src` makes
  the winner deterministic (oldest archived replays, newer stay).
- **An id already live does not abort the batch.** `ON CONFLICT`
  accepts exactly one arbiter index, and it is spent on the ADR-0021
  partial index — a primary-key collision is therefore *not* swallowed
  by it and aborts the whole statement. Since the batch is one
  statement, a single such row would replay none of the window, and
  because the window is deterministic the sweep would retry it forever
  with no supported way to identify or skip the offender. The
  `blocked` anti-join excludes those ids up front, turning a fatal
  abort into a counted, skipped row. (A publish of the same id
  concurrent with this statement still raises — a genuine race, better
  surfaced than swallowed.)
- **`src` is `MATERIALIZED`.** The "oldest archived row wins" rule
  above relies on the INSERT consuming `src` in `(archived_at, id)`
  order. PostgreSQL guarantees the ordering of a query's output, not
  the row-processing order of an inlined CTE; materializing it is what
  makes the guarantee real rather than incidental.
- **The sweep gets its own index (migration V009).** The bulk query is
  `WHERE event_type = ? [AND archived_at …] ORDER BY archived_at, id
  LIMIT ?`, and neither V002 index serves it: `idx_archive_archived_at`
  has no leading `event_type`, `idx_archive_event_type_created_at` is
  ordered by the wrong column. Unserved, the statement walks the archive
  in `archived_at` order filtering on type — while holding the locks of
  its own INSERT into `events` and DELETE from the archive, and now once
  per cursor batch rather than once in total.
  `idx_archive_event_type_archived_at (event_type, archived_at, id)`
  makes both the ordering and the keyset predicate index-resolvable, so
  a batch stays O(limit). The archive is append-only apart from replay
  and purge, so the write cost is one index maintenance per archived
  row — the same trade V002 already took twice.
- Outcome mapping: `found=0` → `NOT_FOUND`; `inserted>0` → `REPLAYED`;
  `id_in_use>0` → `ID_IN_USE`; else `COALESCED`. Bulk:
  `replayed=inserted`, `idInUse=id_in_use`,
  `coalesced=found-inserted-id_in_use`.

### 4. Surfaces

- REST: `POST /events/{id}/replay` (200 with the outcome, 404 when not
  in the archive, 409 for `ID_IN_USE`) and `POST /events/replay-all`. A
  rejected argument — malformed cursor, non-positive limit, dead window
  — is a 400 with the reason, via an `@ExceptionHandler` scoped to the
  controller rather than a `@ControllerAdvice`: an opt-in admin surface
  must not change how the host application renders its own exceptions.
  Actuator needs no equivalent; Spring Boot already maps
  `IllegalArgumentException` out of a `@WriteOperation` to 400.
- Actuator: `action=replay` discriminator on the two existing write
  operations (`POST /{id}` and the bulk `POST`) — Actuator allows one
  `@WriteOperation` per path shape, so the verb is a parameter, the
  same pattern as `purge`'s `target`.
- In-memory adapter has no archive: `NOT_FOUND` / zero counts, same
  carve-out as `findInArchive`/`purgeArchive`.

## Alternatives considered

- **Delete-first CTE (`DELETE ... RETURNING` feeding the INSERT, the
  mirror of the archiving finalize)** — on coalesce the archive row is
  already deleted while the INSERT inserts nothing: the event vanishes
  entirely. Data loss; rejected.
- **Two statements in a transaction** — the same orphan-row race class
  ADR-0008's finalize already rejected, plus the admin runner leases a
  connection per statement; a single CTE needs neither.
- **Deleting the archive row on coalesce** ("the replay was logically
  performed") — loses the audit record of a real past execution and
  buys nothing: the pending event will be archived on its own
  completion. Rejected.
- **A unique index on `archive(event_type, dedup_key)`** — impossible
  by definition (the key recurs across processed runs) and an index
  would tax every archiving INSERT for the sake of rare manual
  investigations; the existing `idx_archive_event_type_created_at`
  covers them. Note this rejects an index on the *key*, not on the
  *sweep's* access path — V009 above adds the latter, which the bulk
  query needs on every call rather than during rare lookups.

## Consequences

- **Migration coupling (upgrade note).** The archiving CTEs now name
  `dedup_key`: a deployment with `archive-enabled=true` that upgrades
  the jar without applying V008 fails every `markProcessed` at runtime.
  Starter-managed Flyway applies V008 automatically; Liquibase users
  must include the `outbox-archive-008-dedup-key` changeset; the
  ADR-0028 legacy-upgrade recipe is unchanged — `baseline-version=7`,
  the highest migration a ≤ 0.4.0 install actually applied. Baselining
  at 8 would mark V008 as already applied and skip it forever, which is
  precisely the broken-`markProcessed` failure above. V009 is an index
  only: skipping it costs bulk-replay performance, not correctness.
- **Operator sweeps are driven by the cursor, not by the counters.**
  Rows that stay archived — coalesced, or their id already live — are
  found again by the same window, so no counter is a usable loop
  condition. `ReplayAllResult.next()` is the `(archived_at, id)` of the
  last row the batch *considered*, whatever its verdict; feed it back
  as `after` and loop until it is `null`. The keyset is the pair, not
  the instant: archive rows can share an `archived_at`, and a
  timestamp-only cursor would skip whichever tied row the previous
  `LIMIT` cut off. The counters are reporting only.
- **A re-published id is reported, not fatal.** When the application
  re-published an archived event's explicit UUID, that row reports
  `ID_IN_USE` (single) or is counted in `idInUse` (bulk); the archive
  row stays and the live event is the one to look at. REST maps it to
  409.
- **Trace context is copied verbatim** — the replayed execution parents
  to the original trace, same as `reenable`.
- **The two new `OutboxAdmin` methods are `default`, not abstract.**
  Their fallback body is the archive-less answer the Javadoc already
  specifies — validate the arguments, then report `NOT_FOUND` / zero
  counts with a null cursor — so a third-party implementation over a
  store without an archive keeps compiling and inherits the documented
  behaviour instead of restating it. Only an adapter that *has* an
  archive overrides them. Validation lives in the default rather than
  in each adapter so the argument contract holds even where there is
  nothing to search; `AbstractOutboxAdminContractTest` asserts it for
  every implementation.
- Pre-1.0 SPI change: `ArchivedEvent` gains a trailing component (a
  break for the canonical constructor), and the new `ArchiveCursor`
  joins `AdminCursor` in the SPI (tracked in the CHANGELOG).

## Related decisions

- [ADR-0008](0008-three-statuses-plus-optional-archive.md) — the
  archive itself; its single-statement finalize CTE is the atomicity
  precedent the replay statement mirrors (in reverse).
- [ADR-0019](0019-admin-and-retention-surface.md) — the `OutboxAdmin`
  port and the two admin surfaces the replay operations extend.
- [ADR-0021](0021-dedup-key-single-inflight-per-key.md) — the dedup
  key and the `ON CONFLICT` arbiter the replay insert reuses; its
  "archive does not carry the key" consequence is superseded here.
- [ADR-0015](0015-at-least-once-semantics.md) — handler idempotency is
  what makes re-execution legal by contract.
- [ADR-0028](0028-starter-managed-flyway-instance.md) — how V008
  reaches databases.
