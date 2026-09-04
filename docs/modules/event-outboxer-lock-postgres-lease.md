# event-outboxer-lock-postgres-lease

The **recommended PostgreSQL `EntityLocker`**
([ADR-0022](../adr/0022-lease-table-postgres-entity-locker.md)): each
held lock is a row in `event_outboxer.entity_locks`, acquired and
released as single autocommit statements. No connection is held while
the handler runs, TTL is honoured, and it is safe behind pgBouncer
transaction pooling.

| | |
|---|---|
| Coordinates | `io.github.bams22:event-outboxer-lock-postgres-lease` |
| Java package | `io.github.bams22.outboxer.lock.postgres.lease` |
| Depends on | [`event-outboxer-api`](event-outboxer-api.md), [`event-outboxer-spi`](event-outboxer-spi.md), `org.postgresql:postgresql` |
| Enable with | `event-outboxer.lock.type: postgres-lease` + migration **V005** |
| Ships | `event-outboxer/migration/lock/V005__outbox_entity_locks.sql` (+ Liquibase changelog) — applied automatically by the starter-managed Flyway instance (ADR-0028) |

## Why it exists

Entity locking serializes handlers per business key
(`EventHandler.extractLockKey`,
[ADR-0012](../adr/0012-extract-lock-key-on-handler.md)). The original
PostgreSQL implementation used session-scoped advisory locks, which
have two structural problems no tuning fixes: one **pinned pooled
connection per held lock** for the whole handler runtime (fleet
self-deadlock risk once `Σ handler-pool-size ≥ maximum-pool-size`),
and **silent loss of mutual exclusion behind pgBouncer**
transaction/statement pooling. The lease table eliminates both while
staying in the same failure domain as the event store — no new
infrastructure. The advisory locker remains available as an
[opt-out](event-outboxer-lock-postgres-advisory.md).

## What it does

**`PgLeaseEntityLocker`** — the only public class:

- **Acquire** — one autocommit `INSERT … ON CONFLICT (lock_key) DO
  UPDATE … WHERE l.expires_at <= now() RETURNING lock_key`: inserts a
  fresh lease or takes over an expired one; zero rows returned means
  *busy* (`Optional.empty()` — never an exception). Each acquisition
  mints a random UUID `owner_token`.
- **Release** — token-guarded compare-and-delete
  (`DELETE … WHERE lock_key = ? AND owner_token = ?`) on a *fresh*
  pool borrow: lock state is a row, not session state, so a different
  physical connection (or pgBouncer backend) is fine. A zero-row
  delete (lease expired / taken over) is a debug-logged no-op — a
  zombie's late release can never free someone else's lease.
- **Clock model** — all expiry arithmetic uses the *database* clock
  (`now()` at write and compare), so a skewed application JVM can
  neither over-hold nor steal a lease.
- **Connection discipline** (correctness, not defense): every borrowed
  connection is forced to autocommit + READ COMMITTED, and every
  statement gets a 5 s query timeout. The starter therefore hands the
  locker the **raw** DataSource, unwrapping any
  `TransactionAwareDataSourceProxy`.
- **Operational extras** — `sweepExpired()` (cosmetic GC of expired
  rows; the starter schedules it every 10 min on the
  `outbox-entity-locks-sweep` daemon thread) and `countLiveLeases()`
  (feeds the `event_outboxer.entity_locks.held` gauge).
- Key length ≤ 512 chars (`entity_locks.lock_key VARCHAR(512)`) —
  validated with a clear error pointing at `extractLockKey()`.

### Guarantees vs the alternatives

| Backend | Exclusion holds until | Crash release | Cost |
|---|---|---|---|
| **postgres-lease** | `min(close(), ttl)` | ≤ `lock-ttl` | 2 short autocommit statements per locked event; zero held connections |
| [postgres-advisory](event-outboxer-lock-postgres-advisory.md) | `close()` or connection loss | clean death: immediate; hard crash: until TCP keepalive (hours) | 1 pinned pooled connection per held lock |
| [redis](event-outboxer-lock-redis.md) | `min(close(), ttl)` | ≤ `lock-ttl` | one Redis key; separate infrastructure |

## When to use it

- **Default choice** for any PostgreSQL deployment where handlers
  declare `extractLockKey`.
- **Mandatory** (among the PG options) behind pgBouncer
  transaction/statement pooling, or when handler pools are large
  relative to the Hikari pool.
- Skip locking entirely (`lock.type: noop`, the default) when no
  handler declares a lock key.

Known trade-off to *not* page on: after a JVM crash, events contending
on the dead holder's key cycle claim → busy → release every
`lock-busy-retry-delay` (default 1 s) until the lease TTL (default
10 m) expires — attempts are not consumed. Raise
`event-outboxer.dispatcher.lock-busy-retry-delay` to shrink the noise
(ADR-0022 §Consequences). With a per-type `lock-wait` (ADR-0035) each
cycle additionally spends that budget on the handler thread before the
release, so a large wait next to a dead holder's lease is the case to
size against.

Under live contention the lease locker inherits the polling bounded
wait of `EntityLocker.tryLock(key, ttl, maxWait)`: every probe is the
same autocommit upsert, issued every 2–10 ms until the lock is obtained
or the type's `lock-wait` is spent. There is deliberately no
`LISTEN/NOTIFY` wake-up here, unlike the Redis locker's pub/sub one:
it was built, measured and removed on 2026-09-05 — no gain over
polling (the lease's own fsync-bound commits are the cycle), a notify
inside the release serialized every release commit of the fleet, and
`LISTEN` is session state that cannot work behind pgBouncer
transaction pooling, the deployment this locker is for. Details in the
[ADR-0022 amendment](../adr/0022-lease-table-postgres-entity-locker.md#amendment-2026-09-05-a-listennotify-wake-up-was-built-measured-and-removed).

## How to configure it

```xml
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-lock-postgres-lease</artifactId>
</dependency>
```

```yaml
event-outboxer:
  lock:
    type: postgres-lease
```

Nothing else: with the module on the classpath the starter-managed
Flyway instance applies V005 on the next start, whatever the state of
the core migrations (ADR-0028).

Notes:

- The starter **fail-fast probes** the `entity_locks` table at startup
  (ordered after the outbox migrations) and names migration V005, the
  classpath location and the Liquibase changelog in the error if it is
  missing — which can only happen with
  `event-outboxer.flyway.enabled=false`.
- The relevant TTL is the per-type
  `event-outboxer.event-types.*.lock-ttl` (default **10 m** = 2 ×
  `handler-max-runtime`; startup enforces `lock-ttl ≥
  handler-max-runtime`). The 2× margin covers zombie handlers and
  JVM-vs-DB clock divergence — keep it.
- Schema follows `event-outboxer.storage.schema` (default
  `event_outboxer`); with several DataSources the locker uses the same
  [`@OutboxDataSource`](../CONFIGURATION.md#selecting-the-datasource-outboxdatasource)
  resolution as the storage adapter.
- `event-outboxer.lock.key-prefix` is **not** used by this locker
  (Redis only) — the raw handler key is stored.
- Migration note when coming from `postgres-advisory`: apply V005
  first; during the rolling deploy old and new pods form disjoint
  exclusion domains (ADR-0022 §Rollout).

### Without Spring

```java
EntityLocker locker = new PgLeaseEntityLocker(rawDataSource);           // schema event_outboxer
// or: new PgLeaseEntityLocker(rawDataSource, "my_schema", "worker-7")
new OutboxEngineBuilder().entityLocker(locker)/*...*/.build();
```

Pass the raw pool, not a transaction-aware proxy, and schedule
`sweepExpired()` yourself if you want the cosmetic cleanup.

## Related

- [STORAGE.md §entity_locks](../STORAGE.md#optional-table-event_outboxerentity_locks) — DDL and admin queries.
- [CONFIGURATION.md §event-outboxer.lock](../CONFIGURATION.md#event-outboxerlock) — all lock properties and the pgBouncer guidance.
- ADRs: [0012](../adr/0012-extract-lock-key-on-handler.md), [0022](../adr/0022-lease-table-postgres-entity-locker.md).
