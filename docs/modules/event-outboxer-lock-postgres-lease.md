# event-outboxer-lock-postgres-lease

The **recommended PostgreSQL `EntityLocker`**
([ADR-0022](../adr/0022-lease-table-postgres-entity-locker.md)): each
held lock is a row in `event_outboxer.entity_locks`, acquired and
released as single autocommit statements. No connection is held while
the handler runs, TTL is honoured, and it is safe behind pgBouncer
transaction pooling — with its opt-in `lock.wakeup` listener left off,
see [Behind pgBouncer](#behind-pgbouncer).

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
  zombie's late release can never free someone else's lease. With
  `releaseNotifications` on, a second autocommit statement on the same
  borrowed connection `pg_notify`s the released key on the schema's
  channel `<schema>.entity_locks` (one channel per schema: a channel
  name is limited to 63 bytes, a key to 512 characters) with
  `synchronous_commit` off for that one transaction. Not inside the
  delete, on purpose: PostgreSQL serializes the commit of every
  notifying transaction in the cluster on one lock, so a notify inside
  the synchronous delete would cap the fleet's release rate at one
  commit latency — about 200/s on fsync-bound storage in the harness.
  The asynchronous notify commit holds that lock for microseconds and
  risks nothing durable: a notification lost to a crash costs one
  fallback probe.
- **Bounded wait with a wake-up (ADR-0035), opt-in** — with
  `releaseNotifications` on (the starter's `lock.wakeup: true`; the
  measured default for this locker is off, see below), the locker
  runs `PgLeaseReleaseListener`: one pooled
  connection held for the locker's life that `LISTEN`s on the channel,
  and a daemon thread that forwards every notified key to the waiters.
  A handler thread that finds a key busy during its type's `lock-wait`
  parks until that key's release arrives instead of re-issuing the
  acquire upsert every 2–10 ms, re-probing every 25 ms
  (`fallbackProbeInterval`) as a safety net for lost notifications and
  leases that expired instead of being released. The listener proves
  the path on every fresh session by sending itself a probe from a
  second, briefly borrowed connection: where the probe never arrives
  — pgBouncer transaction or statement pooling does not forward
  `NOTIFY` — it reports itself unsupported once at WARN, `UNLISTEN`s,
  the locker stops notifying and the wait polls, exactly as before
  (see [Behind pgBouncer](#behind-pgbouncer)); a session lost later is
  reconnected with a back-off, and every reconnect wakes the parked
  waiters so a release missed in the gap costs one probe. A JVM with notifications
  off releases silently, so configure a fleet uniformly — waiters in
  other JVMs then rely on the fallback probe. `close()` stops the
  listener; the starter does that on shutdown.
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
  transaction/statement pooling — with `lock.wakeup` off, its default
  here — or when handler pools are large relative to the Hikari pool.
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
or the type's `lock-wait` is spent.

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
    wakeup: true        # opt-in: LISTEN for release notifications during lock-wait (default off)
```

Nothing else: with the module on the classpath the starter-managed
Flyway instance applies V005 on the next start, whatever the state of
the core migrations (ADR-0028).

Notes:

- `lock.wakeup: true` is off by default for this locker because the
  harness measured no gain from it: on hot keys the lease's own
  acquire and release commits dominate the cycle, and polling every
  2–10 ms adds nothing a notification could remove (274/s polling vs
  275/s with the listener; the virtual-executor cell 53 vs 55/s; see
  the [2026-09-05 addendum](../benchmarks/2026-09-04-laptop-lock-wait.md#addendum-2026-09-05-postgresql-listennotify-wake-up-on-the-lease-locker)).
  It holds **one pool connection** for the application's life (the
  `LISTEN` session) — size the pool for it, and expect a leak-detection
  warning if `leakDetectionThreshold` is set — and costs one extra
  asynchronous round trip per release. Worth trying where commits are
  cheap (no fsync wait) and a hot key's waiters are the bottleneck;
  measure. Behind pgBouncer transaction pooling it reports itself
  unsupported once and polls.

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

### Behind pgBouncer

Acquire and release are single autocommit statements and work in any
pooling mode. **The `lock.wakeup` listener does not: it is unusable
behind pgBouncer in transaction or statement pooling mode.** `LISTEN`
is session state — the subscription lands on whichever server
connection served that one statement, the pooler never forwards the
notifications to the listener's client, and the server connection
keeps the subscription, so a pooler may deliver those notifications to
whatever client it links to that server next, where pgjdbc queues them
unread. What the locker does about it:

- On every fresh session the listener sends itself a probe through a
  second pooled connection and waits up to 2 s for it. A probe that
  never arrives on the first session marks the listener unsupported:
  one WARN (`entity_locks release notifications are not delivered on
  this connection …`), a best-effort `UNLISTEN *` on the same JDBC
  connection (which reaches the right server connection only if the
  pooler links the same one again), no more `NOTIFY` from this JVM,
  and the bounded wait polls exactly as without the option.
- Nothing about locking correctness changes: exclusion, TTL and the
  release path are the same statements as before.

Do not enable `lock.wakeup` behind such a pooler. Where the wake-up is
wanted, the listener needs a session-pooled or direct connection to
PostgreSQL; a dedicated listener URL (the pattern of
`event-outboxer.flyway.url`) is a possible follow-up, not shipped. The
Redis locker's wake-up is unaffected — it rides the Redis connection,
not the JDBC pool.

### Without Spring

```java
EntityLocker locker = new PgLeaseEntityLocker(rawDataSource);           // schema event_outboxer, polling wait
// or: new PgLeaseEntityLocker(rawDataSource, "my_schema", "worker-7")
// with the LISTEN/NOTIFY wake-up:
PgLeaseEntityLocker locker = PgLeaseEntityLocker.builder()
        .dataSource(rawDataSource)
        .schema("my_schema")                 // optional
        .releaseNotifications(true)
        .build();
new OutboxEngineBuilder().entityLocker(locker)/*...*/.build();
// ... and locker.close() on shutdown to stop the listener
```

Pass the raw pool, not a transaction-aware proxy, and schedule
`sweepExpired()` yourself if you want the cosmetic cleanup.

## Related

- [STORAGE.md §entity_locks](../STORAGE.md#optional-table-event_outboxerentity_locks) — DDL and admin queries.
- [CONFIGURATION.md §event-outboxer.lock](../CONFIGURATION.md#event-outboxerlock) — all lock properties and the pgBouncer guidance.
- ADRs: [0012](../adr/0012-extract-lock-key-on-handler.md), [0022](../adr/0022-lease-table-postgres-entity-locker.md), [0035](../adr/0035-bounded-lock-wait.md) (bounded wait and its LISTEN/NOTIFY wake-up).
