# event-outboxer-lock-postgres-advisory

The session-scoped `pg_advisory_lock` `EntityLocker` — the
**pre-[ADR-0022](../adr/0022-lease-table-postgres-entity-locker.md)
implementation, kept as an explicit opt-out** for users who want
immediate lock release on clean process death and accept its costs.
For new deployments prefer
[`event-outboxer-lock-postgres-lease`](event-outboxer-lock-postgres-lease.md).

| | |
|---|---|
| Coordinates | `io.github.bams22:event-outboxer-lock-postgres-advisory` (0.2.0 shipped as `event-outboxer-lock-postgres`) |
| Java package | `io.github.bams22.outboxer.lock.postgres.advisory` |
| Depends on | [`event-outboxer-api`](event-outboxer-api.md), [`event-outboxer-spi`](event-outboxer-spi.md), `org.postgresql:postgresql` |
| Enable with | `event-outboxer.lock.type: postgres-advisory` |
| Migrations | none needed |

## What it does

**`PgAdvisoryLocker`**:

- **Acquire** — borrows a pooled connection and runs
  `SELECT pg_try_advisory_lock(?)` with a hashed key. On success the
  **connection stays checked out inside the `LockHandle`** for the
  whole handler runtime — advisory locks are session-scoped, so
  release must run on the same physical connection.
- **Release** — `SELECT pg_advisory_unlock(?)` on that connection,
  then the connection returns to the pool.
- **Key derivation** — `PgAdvisoryLocker.hash(String)`: SHA-256 of the
  key, first 8 bytes as a signed 64-bit long (public, so you can
  cross-check from SQL). Keys are truncated to 64 bits, and the
  advisory-lock number space is **global per database** — shared with
  any other application using advisory locks.
- **TTL is silently ignored** — PostgreSQL advisory locks have no
  timeout. Exclusion holds until `close()` or connection loss;
  `event-outboxer.event-types.*.lock-ttl` has no effect here.

## When to use it — and when not

Choose it only for one specific property: **immediate release on clean
process death** (the backend sees the TCP EOF and frees the lock at
once, instead of waiting out a lease TTL).

Accept, in exchange:

- **One pinned pooled connection per held lock.** If
  `Σ handler-pool-size ≥ hikari maximum-pool-size`, a saturated fleet
  can deadlock against its own handlers — the starter logs a startup
  WARNING for exactly this ratio.
- **Incompatible with pgBouncer transaction/statement pooling** — lock
  and unlock can land on different server backends, silently losing
  mutual exclusion (a correctness hole, not an error).
- **Hard crash / network partition:** the lock is held until TCP
  keepalive reaps the backend — *hours* with Linux defaults.
- Hashed 64-bit keys in a database-global namespace.

If any of those bite, use the
[lease locker](event-outboxer-lock-postgres-lease.md) (same database,
TTL-bounded crash release) or the
[Redis locker](event-outboxer-lock-redis.md).

## How to configure it

```xml
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-lock-postgres-advisory</artifactId>
</dependency>
```

```yaml
event-outboxer:
  lock:
    type: postgres-advisory   # no migration required
```

Notes:

- **Upgrade note:** before ADR-0022 this backend was selected with
  `lock.type: postgres`. That value no longer binds — startup fails
  listing the valid values, forcing an explicit choice.
- Size the connection pool for
  `expected concurrent held locks + regular load`; keep
  `Σ handler-pool-size` strictly below `maximum-pool-size`.
- With several DataSources the locker follows the same
  [`@OutboxDataSource`](../CONFIGURATION.md#selecting-the-datasource-outboxdatasource)
  resolution as the storage adapter; the starter unwraps any
  transaction-aware proxy so the lock survives the caller's
  commit/rollback boundary.
- `event-outboxer.lock.key-prefix` is not used (the raw key is hashed).

### Without Spring

```java
EntityLocker locker = new PgAdvisoryLocker(rawDataSource);
new OutboxEngineBuilder().entityLocker(locker)/*...*/.build();
```

## Related

- [event-outboxer-lock-postgres-lease](event-outboxer-lock-postgres-lease.md) — the recommended default and the full guarantee comparison.
- [CONFIGURATION.md §Running behind pgBouncer](../CONFIGURATION.md#running-behind-pgbouncer).
- ADRs: [0012](../adr/0012-extract-lock-key-on-handler.md), [0022](../adr/0022-lease-table-postgres-entity-locker.md).
