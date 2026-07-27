# ADR-0022: Lease-table PostgreSQL EntityLocker (default for `lock.type=postgres`)

## Status

Accepted — supersedes the PG-advisory row of the ADR-0012 guarantee
table; `PgAdvisoryLocker` stays available as the `postgres-advisory`
opt-out. Design verified empirically on PostgreSQL 15 (see
§Concurrency semantics); implementation pending.

## Date

2026-07-27

## Context

ADR-0012 introduced the `EntityLocker` SPI: a non-blocking
`tryLock(key, ttl)` the engine calls before invoking a handler that
declares `extractLockKey(payload)`. The MVP PostgreSQL adapter,
`PgAdvisoryLocker`, uses **session-scoped** `pg_try_advisory_lock` and
has two structural problems that no amount of tuning fixes:

1. **One pinned pooled connection (= one PostgreSQL backend process)
   per held lock, for the entire handler runtime.** The acquiring
   `Connection` is captured inside the returned `LockHandle` because
   advisory locks are session state — `pg_advisory_unlock` must run on
   the same physical connection. With
   `Σ handler-pool-size >= maximum-pool-size` a saturated fleet can
   deadlock against its own handlers (locker holds the last
   connection, handler waits for one). The 5ad1c99 startup WARNING
   makes this visible but does not remove it.

2. **Incompatible with pgBouncer in transaction (or statement) pooling
   mode.** A JDBC "session" multiplexes across server connections, so
   the lock and the unlock — or the lock and the handler's own queries
   — land on different backends. The failure mode is silent loss of
   mutual exclusion, not an error.

Secondary irritations of the advisory approach: the `ttl` parameter of
the SPI is silently ignored (no built-in timeout for advisory locks);
keys are truncated through a SHA-256 → signed-bigint hash; and the
advisory lock number space is global per database — shared with any
other application or library using advisory locks.

The ADR-0012 amendment (2026-07-26) already accepts
"exclusion until `min(close(), ttl)`, best-effort without fencing at
the business resource" as the documented guarantee level (the Redis
row of its table). That opens the door to a PostgreSQL implementation
with the same lease semantics — and without session state.

## Alternatives considered

Four candidates were designed independently and judged (unanimous
verdict for A; the full analysis, including an empirical SQL
verification run, is summarized in this ADR):

- **A. Lease table (ShedLock-style) — chosen.** A row in
  `event_outboxer.entity_locks` holds `(lock_key, owner_token,
  expires_at)`; acquire and release are single autocommit statements;
  no session state, no held connections.

- **B. Claim-time exclusion via a persisted `lock_key` column.**
  Persist `lock_key` on `event_outboxer.events` and make the claim
  query skip events whose key has a `PROCESSING` row, enforced by a
  partial unique index `ON (lock_key) WHERE status='PROCESSING'` (a
  plain `NOT EXISTS` prefilter is provably insufficient — classic
  write skew under READ COMMITTED: two claimers of different rows with
  the same key both pass the predicate). Elegant — zero extra round
  trips, zero locker infrastructure, fully pgBouncer-safe — but it
  requires reversing ADR-0012's core decision: the publisher does not
  know the handler, and `extractLockKey` runs worker-side *after*
  deserialization, so there is nothing to put in the column without a
  `PublishOptions.lockKey` escape hatch (explicitly rejected by
  ADR-0012) or publisher-side handler resolution (breaks
  producer-only services). **Rejected for now; recorded as a possible
  post-MVP evolution.**

- **C. Transaction-scoped advisory locks
  (`pg_try_advisory_xact_lock`).** An xact lock releases at
  commit/rollback, so the adapter would have to hold a transaction
  **open** for the whole handler runtime: the same pinned connection
  as today *plus* a minutes-long open transaction (xmin-horizon /
  vacuum damage; killed by `idle_in_transaction_session_timeout`;
  behind pgBouncer the server backend is pinned for the transaction
  anyway, so nothing is gained). The micro-transaction variant
  (acquire in a short transaction and commit immediately) provides no
  exclusion after the commit at all. **Rejected as unsound.**

- **D. Keep session advisory locks on a dedicated direct-to-PG
  DataSource.** Solves the self-deadlock (disjoint pools) but "solves"
  pgBouncer only by evasion: direct PostgreSQL backends exactly where
  the user deployed pgBouncer to avoid them, one per held lock, plus a
  second DSN worth of configuration surface. **Rejected as the
  default; useful fragments (per-mode pool warning, pgBouncer
  documentation) are folded into this ADR.**

## Decision

Add **`PgLeaseEntityLocker`** to `event-outboxer-lock-postgres`
(package `io.github.bams22.outboxer.lock.postgres`, beside
`PgAdvisoryLocker` — ADR-0016 module↔package mapping unchanged, no new
module). `event-outboxer.lock.type=postgres` now selects the lease
locker; the advisory locker remains available as
`lock.type=postgres-advisory`.

The module is deliberately **not** generalized to a dialect-portable
`event-outboxer-lock-jdbc`: the acquire statement relies on
PostgreSQL-specific `INSERT ... ON CONFLICT DO UPDATE ... WHERE ...
RETURNING` semantics (including the EvalPlanQual re-check), and a
dialect-portable lock library is ShedLock's whole product — out of
scope for a PostgreSQL-15+-targeted library.

### Schema (migration V005)

```sql
CREATE TABLE ${eventOutboxerSchema}.entity_locks (
    lock_key     VARCHAR(512) PRIMARY KEY,
    owner_token  VARCHAR(64)  NOT NULL,
    owner_worker VARCHAR(64),
    acquired_at  TIMESTAMPTZ  NOT NULL,
    expires_at   TIMESTAMPTZ  NOT NULL,

    CONSTRAINT entity_locks_expiry_after_acquire CHECK (expires_at > acquired_at)
);
```

- **No secondary index.** The PK serves both acquire and release; the
  stale-row sweep full-scans a table whose size is bounded by
  {currently held leases} + {leases orphaned by crashes} — tens of
  rows.
- `lock_key VARCHAR(512)` follows the `payload_class VARCHAR(512)`
  margin convention and keeps keys inside B-tree tuple limits. The
  adapter pre-validates length and throws `LockAcquisitionException`
  with an explanatory message for longer keys (a deliberate narrowing
  of the de-facto SPI surface — Redis and the advisory hash accepted
  arbitrary lengths).
- `owner_token` stores a random UUID minted **per `tryLock` call**
  (per acquisition, not per worker): two successive acquisitions of
  the same key by the same JVM must carry different tokens so a
  zombie's late `close()` can never release its successor's lease —
  exactly the `RedisEntityLocker` per-call-UUID discipline.
- `owner_worker` is informational only (WorkerId in `claimed_by`
  format, nullable): operator forensics — "which pod holds this
  lease" — without joining anything. Never used in predicates.
- `acquired_at` is not needed for correctness (only `expires_at` is)
  but gives free lease-age observability and feeds the CHECK.
- The table deliberately **ignores `storage.table-prefix`**, matching
  the shipped V001–V004 migrations (the prefix is applied by
  `SchemaResolver` at runtime for the events/workers/archive tables
  the *adapter* queries; migration SQL has never honoured it). The
  schema name comes from the same `${eventOutboxerSchema}` placeholder
  as every other migration.

### Acquire — one statement, autocommit

```sql
INSERT INTO ${schema}.entity_locks AS l
       (lock_key, owner_token, owner_worker, acquired_at, expires_at)
VALUES (?, ?, ?, now(), now() + make_interval(secs => ?))
ON CONFLICT (lock_key) DO UPDATE
   SET owner_token  = EXCLUDED.owner_token,
       owner_worker = EXCLUDED.owner_worker,
       acquired_at  = EXCLUDED.acquired_at,
       expires_at   = EXCLUDED.expires_at
   WHERE l.expires_at <= now()
RETURNING lock_key;
```

`rs.next() == true` → acquired; `false` → busy → `Optional.empty()`.
**Busy is only ever expressed as zero rows, never as an exception.**
The TTL parameter is bound as fractional seconds
(`make_interval(secs => ?)` takes `double precision` — the existing
`findDead` convention in STORAGE.md).

The returned `LockHandle` stores **only `(dataSource, key, token)`** —
never a `Connection`. This is the structural fix for problem (1):
contrast `PgAdvisoryLocker`, which must smuggle the live connection
into its handle.

### Release — token-guarded compare-and-delete

```sql
DELETE FROM ${schema}.entity_locks
WHERE lock_key = ? AND owner_token = ?;
```

Executed on a **fresh** pool borrow — it does not matter that it is a
different physical connection (or a different pgBouncer server
backend) than the acquiring one, because the lock state is a table
row, not session state. This deletes `PgAdvisoryLocker`'s entire
"same-connection invariant" problem class.

- `executeUpdate() == 1` → released.
- `== 0` → the lease expired and was taken over (token mismatch — we
  must not delete the successor's lease) or was already swept:
  `log.debug` and return normally — semantically identical to
  `RedisEntityLocker`'s compare-and-delete Lua script returning 0.
  Wrong-owner protection is structural (the token is in the DELETE
  predicate), including the ABA case.
- `SQLException` → `LockReleaseException`; the dispatcher's
  `closeLock` absorbs it (`onLockReleaseFailed`), and the lease
  self-releases at `expires_at` via the next acquirer's takeover — a
  failed release costs at most `ttl − elapsed` of extra exclusion for
  that key, never a wedged lock.
- Idempotency gate: `volatile boolean closed` checked-then-set before
  any I/O (same pattern as both existing adapters).

### JDBC contract — hard requirements, not defensive measures

Empirical verification (below) showed these are load-bearing:

1. **Autocommit is a hard precondition.** `ON CONFLICT DO UPDATE`
   takes the tuple lock on the conflicting row *before* evaluating the
   `DO UPDATE ... WHERE`, and holds it **until the transaction ends —
   even when the WHERE excludes the row and zero rows are returned**.
   Measured: a busy probe inside a foreign open transaction blocked a
   second prober for 2.4 s and the legitimate holder's release for
   2.1 s. The adapter must check-and-set `setAutoCommit(true)` on
   every borrowed connection, for both acquire and release. Running
   two acquires inside one explicit transaction is forbidden misuse
   (it can deadlock, 40P01 — reproduced; impossible under
   one-statement-per-transaction).
2. **Force `READ COMMITTED` on the borrowed connection** and **map
   `SQLState 40001` to busy** (`Optional.empty()`), not
   `LockAcquisitionException`: under
   `default_transaction_isolation=repeatable read` (a server/pool-level
   GUC users do set) a contended upsert can raise a serialization
   failure even as a single autocommit statement; residual cases must
   degrade to the clean busy path.
3. **`PreparedStatement.setQueryTimeout(~5s)` is normative.** It
   bounds (a) the contender's tuple-lock wait and (b) lease shortening
   — `expires_at` is computed from the *transaction-start* `now()`,
   which predates any row-lock wait, so a contender that waited `W`
   before winning gets an effective lease of `ttl − W`. The timeout
   caps `W`; keep `queryTimeout ≪ lockTtl − handlerMaxRuntime`. Do
   **not** replace it with `SET statement_timeout` — that is
   session-sticky and unsafe under transaction pooling; the JDBC query
   timeout uses an out-of-band cancel (`SQLState 57014`, verified to
   leave the row intact).
4. **TTL floor:** reject `ttl < 1 ms` with
   `IllegalArgumentException`. A sub-millisecond `Duration` binds
   `secs => 0.0`, making `expires_at == acquired_at`, which trips the
   CHECK as a confusing `SQLState 23514`. Unreachable through the
   engine (`lockTtl >= handlerMaxRuntime` is enforced) but reachable
   through the bare SPI.
5. **Key validation:** non-null, length ≤ 512, ttl positive — same
   guard style as `RedisEntityLocker`.
6. **Error envelope for callers:** busy = 0 rows only; expected
   exceptional SQLSTATEs are 57014 (query cancel), 40001 (mapped to
   busy per item 2), 23514 (only via the sub-ms bug, excluded by item
   4). Any `SQLException` on acquire →
   `LockAcquisitionException` → the dispatcher reschedules the event
   with `lock-busy-retry-delay` **without consuming an attempt**
   (existing `HandlerDispatcher.tryAcquireLock` behaviour) — a flaky
   lock backend degrades gracefully.

### Concurrency semantics (verified empirically on PostgreSQL 15.17)

The design was raced on a live PostgreSQL 15 instance (two-session
races with transactions held open, sweep interleavings both
directions, and 32-thread stampedes on absent and expired keys). All
verdicts held:

- The acquire is atomic via PostgreSQL's speculative-insertion
  protocol; it can never raise a unique violation. Insert/insert race
  on an absent key: exactly one winner; the loser re-enters via the
  conflict arm, re-evaluates the WHERE against the committed row →
  busy. Takeover race on an expired key: contenders queue on the row
  lock; the first wins; each subsequent contender re-evaluates under
  EvalPlanQual against the new row version → busy. Exactly one winner
  in every run; zero error paths; zero deadlocks under the intended
  protocol.
- Concurrent sweep vs acquire is safe in both interleavings: the
  conflict arm restarts from the insert arm if the row vanished
  (documented PG behaviour); the sweep's WHERE re-check skips a
  concurrently renewed row.
- **`tryLock` is bounded-blocking, not never-blocking:** a contender
  (on any contended key — live or expired; a busy probe is not a pure
  read, it writes a tuple lock) waits for the remainder of another
  contender's *single statement + commit* — sub-millisecond work plus
  an fsync, never the handler runtime. A stampede of N contenders on
  one hot key serializes through N short commits. This is an accepted
  semantic softening of `pg_try_advisory_lock`'s never-blocking
  behaviour; the SPI's "non-blocking" contract is read as "never
  blocks for handler-scale durations".
- **Conservative spurious busy:** a contender that queued behind
  another transaction re-evaluates the WHERE with its own
  transaction-start `now()`, so it can report busy for a lease that
  has already wall-clock expired by decision time. Direction is
  strictly safe (a live lease can never be stolen early via this
  path) and it self-heals on the next `lock-busy-retry-delay` attempt.

### Clock model

All expiry arithmetic uses the **database clock** (`now()` both when
computing and when comparing `expires_at`) — the workers-heartbeat
precedent. A skewed application JVM can neither hold a lease too long
nor steal one early. Two caveats are documented rather than solved:

- The watchdog measures `handlerMaxRuntime` with the **engine JVM
  clock** while lease expiry runs on the DB clock; zombie-overlap
  confinement therefore rests on `lockTtl >= 2× handlerMaxRuntime`
  absorbing the JVM-vs-DB divergence (drift, GC-delayed watchdog, an
  NTP step on the DB host < the 5 m margin). Keep NTP discipline on
  the database host; note that failover to a clock-skewed standby can
  expire live leases early (the same trust the heartbeat table already
  extends at `dead-threshold` scale, now on the exclusion path).

### Guarantee table (updates the ADR-0012 amendment)

| Backend | Exclusion holds until | Crash release | Cost |
|---|---|---|---|
| Redis/KeyDB (`SET NX PX` + token-checked release) | `min(close(), ttl)` | TTL expiry (≤ ttl) | one Redis key |
| **PostgreSQL lease (`entity_locks`) — new default** | `min(close(), ttl)` | lease expiry (≤ ttl) | 2 short autocommit statements per locked event; **zero** connections held during the handler |
| PostgreSQL advisory (session-scoped, `postgres-advisory`) | `close()` or connection loss | **clean process death:** immediate (backend sees EOF); **hard crash / partition:** until TCP keepalive reaps the backend — *hours* with Linux defaults | one pinned pooled connection per held lock |

The advisory row's original "connection drop, immediate" was true only
for clean process death; the split above corrects it. The lease's
`≤ ttl` bound is worse than advisory only in the clean-crash case —
which materially strengthens the default flip. (The current
`PgAdvisoryLocker` javadoc claim that a stuck handler's connection "is
eventually recycled by the pool" is wrong — HikariCP never evicts
checked-out connections — and must be fixed in the implementation PR.)

### Starter integration

- `OutboxProperties.LockType` gains `postgres_advisory` (bound from
  `postgres-advisory` via relaxed binding). Lease-vs-advisory
  selection **branches on the bound enum inside one
  `PostgresLockAutoConfiguration`** (condition: adapter class on the
  classpath + `DataSource` bean + `lock.type` ∈
  {`postgres`, `postgres-advisory`}), *not* on raw
  `@ConditionalOnProperty(havingValue=...)` string matching — a user
  writing `postgres_advisory` in YAML binds fine to the enum but fails
  a raw string gate, yielding a cryptic missing-`EntityLocker`
  context failure. Both lockers keep taking the **raw** `DataSource`
  (never the transaction-aware proxy): lock statements must not join
  the caller's transaction — for the lease locker this is a
  correctness requirement (JDBC contract item 1), not a preference.
- **Fail-fast probe:** at startup the starter runs
  `SELECT 1 FROM <schema>.entity_locks LIMIT 1` and converts a missing
  table into an actionable error naming migration V005 and the
  `postgres-advisory` escape hatch. The probe bean is gated behind
  database initialization (`@DependsOnDatabaseInitialization` or
  equivalent ordering after Flyway/Liquibase) so the first deploy that
  adds V005 does not race its own migration.
- `warnIfPgLockCanExhaustPool` (the 5ad1c99 pool-starvation WARNING)
  re-gates to `postgres-advisory` only — the self-deadlock scenario is
  structurally impossible in lease mode — and its message text is
  updated to name the mode.
- **Sweep:** the adapter exposes a public `sweepExpired()`
  (`DELETE FROM entity_locks WHERE expires_at <= now()`); the starter
  schedules it on a dedicated single-thread daemon executor at a fixed
  10-minute cadence (not configurable in MVP). Correctness never
  depends on the sweep — expired rows are overwritten in place by the
  next acquirer; the sweep only garbage-collects rows of keys that are
  never contended again — so any cadence is safe. (Core's
  `MaintenanceScheduler` has no task-registration hook; adding one for
  a cosmetic cleanup is not justified.)
- **Observability:** a Micrometer gauge for currently held leases
  (`SELECT count(*) FROM entity_locks WHERE expires_at > now()`) under
  the `event_outboxer` metrics prefix, wired through the existing
  metrics adapter.
- The lease table's schema comes from `event-outboxer.storage.schema`
  (lock and storage share one database in practice; no separate
  `lock.*` schema override in MVP).

### Migrations

`V005__outbox_entity_locks.sql` ships in
`event-outboxer-lock-postgres` under a new opt-in location
`classpath:db/migration/outbox/lock` (same `${eventOutboxerSchema}`
placeholder; V005 continues the shared numbering sequence — core:
V001/V003/V004, archive: V002 — so aggregated Flyway locations never
collide). A Liquibase wrapper
`classpath:db/changelog/outbox/lock/changelog.xml` is added the same
way the core/archive changelogs are.

**Out-of-order caveat:** adopting the lock location *after* a future
core migration (V006+) has been applied is an out-of-order Flyway
migration and fails validation without `flyway.out-of-order=true` —
the same pre-existing hazard as late archive (V002) adoption. Release
notes and STORAGE.md state the rule: enable the location at upgrade
time.

### Testing

- `PgLeaseEntityLockerIT` extends the existing
  `AbstractEntityLockerContractTest` **unchanged** (verified: all six
  contract tests pass `Duration.ofSeconds(30)` and never exercise TTL
  expiry, so DB-clock expiry cannot conflict with `SettableClock`;
  the 32-thread exclusivity test resolves through speculative
  insertion + EvalPlanQual to exactly one winner).
- The contract test gains an **opt-in, Assumptions-gated
  `forceExpire(key)` hook** (default: not supported → TTL tests
  skipped) so TTL-honouring lockers can finally have expiry semantics
  contract-tested; the PG implementation backdates *both*
  `acquired_at` and `expires_at` (the CHECK requires it).
- **Problem-(1) regression IT:** with the application pool saturated
  by handlers, lease acquisition must degrade to
  `LockAcquisitionException → reschedule` (bounded by
  `connectionTimeout`), never deadlock the fleet.
- Concurrency IT racing concurrent acquires on live/expired/absent
  keys (the empirical scenarios above, automated).

### Rollout for existing users

`lock.type=postgres` changes meaning in place (safe-by-default: the
advisory mode's pgBouncer failure is silent-exclusion-loss grade, its
pool coupling is fleet-deadlock grade; users who want the old
behaviour set `postgres-advisory`). During a rolling deploy old pods
(advisory) and new pods (lease) form **disjoint exclusion domains** —
per-key serialization is best-effort across the fleet for the rollout
window. Guidance: apply V005 before the deploy (the fail-fast probe
enforces this), flip during low traffic, and treat the window like the
zombie-overlap case the ADR-0012 amendment already documents.

## Rationale

- The lease lands on exactly the guarantee level the project already
  accepts for Redis (`min(close, ttl)`, best-effort without fencing at
  the business resource), while structurally eliminating both stated
  problems: zero connections held during the handler and full
  transaction-pooling compatibility (both operations are
  single-statement autocommit with all state in a table row — even
  statement pooling works).
- Outbox *state* is fenced regardless of locker choice: finalize
  checks `version + claimed_by` (ADR-0014), so a zombie can never
  finalize; the lease's token-guarded DELETE additionally guarantees a
  zombie's late `close()` cannot release its successor's lease.
- The enforced `lockTtl >= handlerMaxRuntime` (default 2×) contract —
  added precisely to make TTL the crash-release mechanism — finally
  has a PostgreSQL backend that honours it.
- No new infrastructure and a shared failure domain: the lock backend
  is the same database as the event store — if it is down, the outbox
  is down anyway. Single-database users no longer need Redis to get a
  correct locker behind pgBouncer.
- Keys become exact, human-readable strings namespaced by
  schema+table: no 64-bit hash truncation, no collisions with other
  applications' advisory locks.

## Consequences

### Positive

- Self-deadlock (`Σ handler-pool-size` vs `maximum-pool-size`)
  becomes structurally impossible in the default PG mode; the startup
  WARNING is retired to `postgres-advisory`.
- Works behind pgBouncer transaction pooling — previously the
  documented answer was "use Redis".
- TTL honoured; SPI contract and `HandlerDispatcher` need **zero
  changes**; the existing contract test passes unchanged.
- Live leases are observable (`entity_locks` rows + Micrometer gauge)
  where advisory locks were invisible outside `pg_locks`.

### Negative

- **Clean-crash release degrades** from immediate (advisory, backend
  EOF) to bounded-by-ttl (default 10 m). After a JVM crash, events
  contending on the dead holder's key are rescheduled every
  `lock-busy-retry-delay` (default 1 s) until the lease expires:
  orphan recovery returns the dead worker's events after ~30 s, so the
  worst case is ~9.5 minutes of claim → busy → release cycles per
  blocked key — hundreds of version-bumping UPDATEs and
  `onLockAcquisitionFailed` emissions. Operators should expect that
  volume (do not page on it); raising `lock-busy-retry-delay` shrinks
  it. A capped exponential backoff for the lock-busy path is a
  candidate post-MVP knob.
- `tryLock` is bounded-blocking (millisecond-scale tuple-lock waits
  under contention) instead of advisory's never-blocking probe.
- Two extra pool borrows and two WAL-writing transactions per locked
  event; dead-tuple churn on hot lock keys (the table is tiny —
  default autovacuum suffices).
- Under total pool saturation, acquire *and release* can stall up to
  HikariCP `connectionTimeout` (default 30 s) on the handler thread —
  and a failed release extends exclusion for that key until `ttl`.
  Redis does not share this failure mode (its release rides a
  dedicated Lettuce connection). Safety is unaffected (`closeLock`
  absorbs the failure); this is a liveness delta only.
- Key length bounded at 512 characters (validated, clear error).
- Exclusion now trusts the PostgreSQL server clock (NTP step-back
  extends effective leases; step-forward can expire live ones early).
- Rolling-deploy backend switch: temporary split-brain of exclusion
  domains (see §Rollout).
- Stale rows of crashed holders on never-again-contended keys persist
  until the sweep — a small new maintenance surface advisory and Redis
  do not have.

## Post-MVP path

- **Lease renewal** (heartbeat extension):
  `UPDATE entity_locks SET expires_at = now() + make_interval(secs => ?)
  WHERE lock_key = ? AND owner_token = ?` at ~`ttl/3`, allowing much
  shorter TTLs (faster crash release) without risking legitimate
  long handlers. Open: where the renewal scheduler lives, given
  adapters have no threads today.
- **Claim-time exclusion** (alternative B) as an evolution that
  removes even the two lock round-trips — requires the ADR-0012
  reversal documented above.
- Capped backoff for the lock-busy retry path.
- Admin surface: expose live leases (list / force-release) through the
  `OutboxAdmin` SPI and the actuator endpoint (ADR-0019).

## Related decisions

- [ADR-0012](0012-extract-lock-key-on-handler.md) — lock-key source
  and the per-backend guarantee table this ADR updates.
- [ADR-0006](0006-no-listen-notify-in-mvp.md) — its pgBouncer claims
  are re-scoped per lock backend by this ADR.
- [ADR-0014](0014-optimistic-locking-via-version-field.md) — the
  version/claimed_by fence that keeps outbox state safe regardless of
  locker choice.
- [ADR-0005](0005-workers-heartbeat-table.md) — the DB-clock
  precedent the lease reuses.
- [ADR-0016](0016-maven-module-structure.md) — module placement.
