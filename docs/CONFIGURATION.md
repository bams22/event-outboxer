# Configuration

Full reference for `application.yml` used by
`event-outboxer-spring-boot-starter`. Every property below is bound by
`OutboxProperties` (prefix `event-outboxer`); defaults shown are the
values the starter actually applies.

## Contents

1. [Quick start](#quick-start)
2. [Full property tree](#full-property-tree)
3. [Section reference](#section-reference)
4. [Per-type override (thin merge)](#per-type-override-thin-merge)
5. [Invariant validation](#invariant-validation)
6. [Overriding through Java code](#overriding-through-java-code)

---

## Quick start

Minimal configuration for PostgreSQL:

```yaml
event-outboxer:
  storage:
    type: postgres      # required — there is no default (ADR-0020)
```

Everything else comes from the defaults. The outbox schema is migrated
by the starter's own Flyway instance as long as `flyway-core` and
`flyway-database-postgresql` are on the classpath — nothing to add to
`spring.flyway.locations` (see
[`event-outboxer.flyway.*`](#event-outboxerflyway), ADR-0028). To add
the PostgreSQL-backed entity lock, set `event-outboxer.lock.type:
postgres-lease` (its migration is applied automatically); for the
Redis/KeyDB lock, set `event-outboxer.lock.type: redis` and point
`event-outboxer.redis.uri` (or `.host`) at your Redis — the starter
creates the Lettuce connection itself (see
[`event-outboxer.redis.*`](#event-outboxerredis) and
[`event-outboxer.lock.*`](#event-outboxerlock)).

---

## Full property tree

```yaml
event-outboxer:
  enabled: true                      # master switch, default=true
  publish-only: false                # true = no pollers on this instance; EventHandler beans optional (ADR-0029)

  worker:
    id: null                         # override WorkerId; null = {hostname}-{pid}-{uuid8}
    host: null                       # override hostname; null = resolved automatically
    metadata:                        # arbitrary Map<String,String> → event_outboxer.workers.metadata JSONB
      app: my-service
      version: ${git.commit.sha:unknown}

  publisher:
    no-transaction-policy: FAIL      # FAIL | IGNORE

  storage:
    type: postgres                   # REQUIRED — no default, no in-memory option (ADR-0020)
    # schema is shared between the adapter (SQL) and the classpath
    # migrations (Flyway ${eventOutboxerSchema} placeholder). Default
    # name is specific to avoid conflicts with other libraries.
    schema: event_outboxer
    table-prefix: ""                 # optional table-name prefix (event_outboxer.<prefix>events)
    archive-enabled: false           # move successful events to the archive table
    metrics-cache-ttl: 30s           # TTL of the metricsSnapshot() cache

  flyway:                            # starter-managed Flyway instance (ADR-0028); locations are fixed
    enabled: true                    # false = apply the shipped SQL through your own tooling
    url: null                        # dedicated migration connection; null = the outbox DataSource
    user: null                       # with url unset: derive a connection from the outbox DataSource
    password: null
    driver-class-name: null          # null = detected from url
    baseline-on-migrate: false       # one-time upgrade from ≤ 0.4.0 installations (see CHANGELOG)
    baseline-version: "1"            # version recorded by that baseline (7 = everything up to 0.4.0)

  redis:                             # starter-managed Lettuce connection (ADR-0027);
    uri: null                        # full RedisURI; wins over the discrete fields below
    host: null                       # either uri or host activates the managed connection
    port: 6379
    username: null                   # ACL user; requires password
    password: null
    database: 0
    ssl: false
    timeout: null                    # connect/command timeout; null = Lettuce default (60s)
    client-name: null                # CLIENT SETNAME

  lock:
    type: noop                       # noop (default) | postgres-lease | postgres-advisory | redis
    key-prefix: "outbox:lock:"

  cache:
    type: memory                     # memory (default) | redis | noop
    redis:
      key-prefix: "outbox:metrics:"  # shared key namespace when type=redis

  event-types:
    defaults:
      poll-min-interval: 500ms       # floor of the adaptive poll interval
      poll-max-interval: 10s         # ceiling of the adaptive poll interval
      poll-multiplier: 1.5           # growth factor after an empty poll; must be > 1.0
      claim-batch-size: 10           # events claimed per poll
      handler-pool-size: 3           # fixed per-type thread pool (core == max, no scaling)
      handler-queue-capacity: 100    # bounded queue; 0 = synchronous handoff (fail fast)
      handler-max-runtime: 5m        # watchdog threshold for a stuck handler
      interrupt-stuck-handler: true  # interrupt the handler thread after force-reclaim
      lock-ttl: 10m                  # entity-lock TTL; must be >= handler-max-runtime (2x recommended)
      failure:                       # retry policy — thin merge like the fields above (ADR-0030)
        strategy: exponential        # exponential (default) | fixed | none
        max-attempts: 10             # then exhausted-action; ignored for strategy none
        exhausted-action: DISABLE    # DISABLE | DELETE
        base-delay: 5s               # exponential: delay before the first retry
        multiplier: 2.0              # exponential: growth per attempt, > 1.0
        max-delay: 1h                # exponential: cap, >= base-delay
        jitter: 0.2                  # exponential: random fraction in [0, 1]
        fixed-delay: 30s             # fixed only
        log-level: WARN              # TRACE..FATAL | OFF (no log line per failure)
    overrides:                       # thin merge: set only the fields you change
      SEND_EMAIL:
        handler-pool-size: 20
        poll-min-interval: 2s
        failure:
          max-attempts: 5            # only this key changes; the rest comes from defaults.failure
          base-delay: 30s
      UPDATE_CACHE:
        handler-pool-size: 30
        poll-min-interval: 500ms
        handler-max-runtime: 1m

  dispatcher:
    unknown-handler-policy: SKIP     # SKIP (default) | DISABLE | FAIL
    unknown-handler-retry-delay: 1m  # reschedule delay when policy=SKIP
    lock-busy-retry-delay: 1s        # reschedule delay when the entity lock is busy
    dispatch-rejected-retry-delay: 1s # reschedule delay when the handler executor is saturated
    finalize-batching: true          # group-commit batching of finalize statements
    finalize-batch-max-size: 128     # cap on rows per flushed finalize statement

  maintenance:
    heartbeat-interval: 5s           # how often the worker refreshes event_outboxer.workers
    dead-threshold: 30s              # heartbeat silence before a worker counts as dead
    orphan-recovery-interval: 30s    # orphan-recovery task period
    watchdog-interval: 10s           # stuck-handler watchdog period
    abandoned-handler-grace: 30s     # after this, a force-reclaimed thread counts as lost
    reclaim-batch-size: 50           # max dead workers processed per orphan-recovery pass
    shutdown-timeout: 30s            # max wait for in-flight handlers on shutdown
    stale-claim-threshold: null      # null = derived: 2 × max handler-max-runtime
    stale-claim-sweep-interval: 5m   # stale-claim sweeper period

  handler-executor:
    type: platform                   # platform | virtual

  metrics:
    # Prefix applied to every Micrometer counter/timer/summary. Default
    # chosen to avoid clashing with other libraries that publish outbox.*.
    prefix: event_outboxer

  tracing:
    # Auto-detection of the OutboxTracer adapters (ADR-0023). false disables
    # both; a user-defined OutboxTracer bean is honoured regardless.
    enabled: true
    # Span shape of a deferred event (runAt further ahead than link-threshold
    # at publish time). link = the consumer span starts a new trace and links
    # to the producer span; child = always parent-child, however far ahead.
    deferred-propagation: link
    # How far ahead runAt must lie for an event to count as deferred.
    # 0s links every event with an explicit future runAt.
    link-threshold: 1m

  health:
    # Merge the outbox indicator into these Actuator health groups. Default
    # empty = no influence on /actuator/health/liveness or /readiness.
    # For k8s rolling restart: [readiness] is the recommended minimum.
    probe-groups: []

  retention:
    archive-older-than: null         # e.g. 30d; null (default) = archive retention off
    disabled-older-than: null        # e.g. 90d; null (default) = DISABLED retention off
    batch-size: 1000                 # rows per DELETE; a pass loops until a short batch
    interval: 1h                     # delay between retention passes

  admin:
    rest:                            # requires the event-outboxer-admin-rest module
      enabled: false                 # write-capable HTTP surface — strictly opt-in
      base-path: /outbox-admin
      required-authority: OUTBOX_ADMIN
      enforce-authority: true        # fail startup if @PreAuthorize would be silently ignored
```

---

## Section reference

### `event-outboxer.enabled`

Master switch. `false` — the library does not activate, no beans, no
pollers. Useful for dev/staging environments where the outbox is not
needed.

### `event-outboxer.worker.*`

- `id` — override for the autogenerated WorkerId. By default it is
  generated as `{hostname}-{pid}-{uuid8}`
  (e.g. `api-srv-01-4817-a3f2b1c9`). Typically untouched.
- `host` — explicit hostname stored in the worker registry; resolved
  automatically when unset.
- `metadata` — an arbitrary `Map<String,String>` written to
  `event_outboxer.workers.metadata JSONB` on registration. Useful for
  debugging: application version, git-sha, environment, image tag.

### `event-outboxer.publisher.no-transaction-policy`

Behavior of `OutboxEventPublisher.publish()` when called outside an
active transaction:

- `FAIL` (default) — `NoTransactionException` is thrown. Safe: prevents
  accidentally publishing without atomicity.
- `IGNORE` — writes without a surrounding transaction. Unsafe, for
  tests only.

### `event-outboxer.publish-only`

Default `false`: the engine requires at least one `EventHandler` bean
and otherwise fails at startup with a `FailureAnalyzer` diagnosis —
events persisted by an application that never processes them are
almost always a wiring mistake (handlers outside component scan, wrong
profile, missing `@Component`).

`true` declares the instance **publish-only** (ADR-0029): the engine
registers its worker, runs heartbeat / orphan recovery / retention and
exposes `OutboxEventPublisher`, but starts no pollers; any
`EventHandler` beans present are ignored. Use it for a service that
only emits events, or to run one code base as API nodes
(`publish-only: true`) and worker nodes (`publish-only: false`).

### `event-outboxer.storage.*`

Storage adapter settings.

- `type` — **required, no default** (ADR-0020). The only value is
  `postgres` (requires a `DataSource` bean and the
  `event-outboxer-storage-postgres` dependency; with several
  `DataSource` beans, see
  [Selecting the DataSource](#selecting-the-datasource-outboxdatasource)).
  There is deliberately
  no in-memory option: a silently non-durable outbox would not
  participate in your transactions and would lose events on restart —
  the exact failure this library exists to prevent. An unconfigured
  outbox fails at startup with an actionable message. For tests, see
  [Testing without a database](#testing-without-a-database).
- `schema` — schema name. **Default: `event_outboxer`** — a specific
  name chosen to avoid clashing with other libraries or application
  tables in a shared database. The value is propagated into the
  adapter's SQL, into the starter-managed Flyway instance (`schemas` +
  the `${eventOutboxerSchema}` placeholder, so the history table lives
  in this schema too) and into the application's Flyway/Liquibase
  placeholder for the opt-out path, so changing it once updates all.
- `table-prefix` — optional table prefix (e.g. `v1_` →
  `event_outboxer.v1_events`).
- `archive-enabled` — enables archiving of successful events (ADR-0008).
  The archive table itself is always created by the starter-managed
  migrations; the flag only governs runtime behaviour.
- `metrics-cache-ttl` — TTL applied by the default in-memory cache and
  (when `event-outboxer.cache.type=redis`) as the PX expire on the Redis key.
  Ignored when `event-outboxer.cache.type=noop` or a custom
  `@Bean MetricsSnapshotCache` takes over.

### `event-outboxer.flyway.*`

The starter-managed Flyway instance that applies the library's own
schema migrations (ADR-0028). Active when `flyway-core` (+
`flyway-database-postgresql`) and `event-outboxer-storage-postgres`
are on the classpath and `storage.type=postgres`. It is independent of
the application's `spring.flyway.*` instance:

- **locations are fixed** — `classpath:event-outboxer/migration/core`
  and `/archive` always, `/lock` whenever
  `event-outboxer-lock-postgres-lease` is present. They are not read
  from properties and must not be added to `spring.flyway.locations`
  (they live outside `db/migration/` precisely so the application's
  instance never scans them);
- **own history table** — `flyway_schema_history` inside
  `event-outboxer.storage.schema`; the application's history table and
  version sequence are untouched;
- `outOfOrder` is on: a lane adopted later (the lease module added
  after core migrations ran) applies without a validation error.

Properties:

- `enabled` — default `true`. `false` disables the instance; apply the
  shipped SQL yourself (Flyway with the locations above — the
  `${eventOutboxerSchema}` placeholder is still fed into the
  application's instance — or the Liquibase changelogs under
  `db/changelog/outbox/*`).
- `url` — JDBC URL of a dedicated migration connection, e.g. for a DDL
  role separate from the application role. Default `null`: migrate
  through the outbox `DataSource` (`@OutboxDataSource`-qualified bean,
  else the unique / `@Primary` one — ADR-0024; the transaction-aware
  proxy is unwrapped).
- `user`, `password` — credentials for `url`. With `url` unset, a
  non-null `user` derives a connection from the outbox `DataSource`
  with these credentials (same precedence as `spring.flyway.user`).
- `driver-class-name` — driver for `url`; default detected from the URL.
- `baseline-on-migrate`, `baseline-version` — one-time upgrade from an
  installation that applied the outbox migrations through the
  application's instance (≤ 0.4.0): set `baseline-on-migrate: true`
  and `baseline-version: 7` (the highest outbox migration already
  applied) for one deploy, then remove both. A first start against
  such a schema fails with this recipe in the message. See the
  CHANGELOG upgrade notes.

```yaml
event-outboxer:
  flyway:
    url: jdbc:postgresql://db:5432/orders
    user: outbox_migrator
    password: ${OUTBOX_MIGRATOR_PASSWORD}
```

Flyway 10+ without `flyway-database-postgresql` on the classpath fails
fast at startup naming the artifact.

### `event-outboxer.redis.*`

Starter-managed Lettuce connection (ADR-0027) shared by the Redis
entity locker (`lock.type=redis`) and the Redis metrics cache
(`cache.type=redis`). When `uri` or `host` is set — and the
application defines no `StatefulRedisConnection` bean of its own —
the starter creates a `RedisClient` + `StatefulRedisConnection<String,
String>` at startup, exposes the connection as the bean
`outboxRedisConnection` carrying
[`@OutboxRedisConnection`](#selecting-the-redis-connection-outboxredisconnection),
and closes both on context shutdown (connection first, then client).

- `uri` — full Lettuce `RedisURI`, e.g. `redis://localhost:6379/0` or
  `redis-sentinel://host1,host2/0#mymaster`. **Wins over the discrete
  fields** when both are set (mirrors `spring.data.redis.url`).
- `host` / `port` / `database` / `ssl` — discrete alternative to
  `uri`.
- `username` / `password` — ACL (`AUTH user pass`) when both set;
  password-only `AUTH` when just `password`.
- `timeout` — connect and command timeout; `null` keeps Lettuce's
  60-second default. The connection is opened eagerly, so this also
  bounds how long a down Redis can block application startup.
- `client-name` — reported via `CLIENT SETNAME`.

A user-defined `StatefulRedisConnection` bean always wins: the starter
then creates nothing and these properties are inert. Redis Cluster or
custom `ClientResources` are deliberately not covered — define your
own connection bean for those.

### `event-outboxer.lock.*`

`EntityLocker` selection. There is no classpath auto-detection — the
default is `noop` and other backends are opt-in:

- `type: noop` (**default**) — no business-key locking.
- `type: postgres-lease` — **lease-table locker** (`PgLeaseEntityLocker`,
  ADR-0022): a row in `event_outboxer.entity_locks` per held lock,
  acquire/release as single autocommit statements. Requires
  `event-outboxer-lock-postgres-lease` on the classpath, a `DataSource`
  bean (with several, see
  [Selecting the DataSource](#selecting-the-datasource-outboxdatasource)),
  and migration V005 — applied automatically by the starter-managed
  Flyway instance (ADR-0028; with `event-outboxer.flyway.enabled=false`
  use the location `classpath:event-outboxer/migration/lock` or the
  Liquibase changelog `db/changelog/outbox/lock/changelog.xml`). The
  starter fail-fast probes the table at startup and names the
  migration in the error if it is missing. Holds **no** connection while the handler runs and is
  safe behind pgBouncer transaction pooling. TTL (`lock-ttl`) is
  honoured: crash release ≤ ttl.
- `type: postgres-advisory` — the pre-ADR-0022 session-scoped
  `pg_advisory_lock` locker (`event-outboxer-lock-postgres-advisory` on the
  classpath). Kept for users who want immediate lock
  release on clean process death and accept the costs: one pinned
  pooled connection per held lock (the starter warns when
  `Σ handler-pool-size >= maximum-pool-size` — self-deadlock risk),
  `lock-ttl` ignored, **incompatible with pgBouncer
  transaction/statement pooling**, and after a hard crash (power loss,
  network partition) the lock is held until TCP keepalive reaps the
  backend — hours with Linux defaults.
- `type: redis` — Redis/KeyDB locker; requires
  `event-outboxer-lock-redis` on the classpath and a
  `StatefulRedisConnection<String, String>` — either starter-managed
  via [`event-outboxer.redis.*`](#event-outboxerredis) (ADR-0027) or a
  user-provided bean (with several, see
  [Selecting the Redis connection](#selecting-the-redis-connection-outboxredisconnection)).
  With neither, startup fails fast naming both remedies — `redis` is
  an explicit opt-in, so there is no silent back-off.
- `key-prefix` — prefix for lock keys, default `outbox:lock:`
  (Redis locker only; the PG lockers store/hash the raw key).

Upgrade note: before ADR-0022 there was a single `type: postgres`
value (the advisory locker). It was split into `postgres-lease` and
`postgres-advisory`; the old `postgres` value no longer binds —
startup fails listing the valid values, forcing an explicit choice
instead of a silent semantics change. When moving a fleet from
advisory to lease, apply V005 first; during the rolling deploy old
and new pods form disjoint exclusion domains for the rollout window
(see ADR-0022 §Rollout).

#### Running behind pgBouncer

With pgBouncer in **transaction** (or statement) pooling mode:

- Polling, claim, finalize, heartbeat and the `postgres-lease` and
  `redis` lockers are safe — no session state.
- `postgres-advisory` is **not** usable: session-scoped advisory locks
  silently lose mutual exclusion when statements multiplex across
  server connections. Use the lease locker, the Redis locker, or a
  direct/session-pooled connection.
- pgJDBC's server-side prepared statements (`prepareThreshold`,
  default 5) conflict with transaction pooling on pgBouncer < 1.21:
  either set `prepareThreshold=0` on the JDBC URL or run pgBouncer ≥
  1.21 with `max_prepared_statements` enabled. This applies to the
  whole storage adapter, not just locking. (Reasoned guidance, not
  empirically exercised by the library's test suite.)

### `event-outboxer.cache.*`

Backs `EventStore.metricsSnapshot()` caching. See
[docs/STORAGE.md §Pluggable metrics cache](STORAGE.md#pluggable-metrics-cache)
for the motivation (consistent snapshot across pods) and the full
Redis wiring recipe.

- `type` — one of:
  - `memory` (default) — per-JVM `AtomicReference` TTL cache, keyed off
    `event-outboxer.storage.metrics-cache-ttl`.
  - `noop` — caching disabled; every `metricsSnapshot()` call hits the
    database. Useful for tests that need live state.
  - `redis` — shared Redis/KeyDB-backed cache; requires
    `event-outboxer-cache-redis` on the classpath and a
    `StatefulRedisConnection<String, String>` — starter-managed via
    [`event-outboxer.redis.*`](#event-outboxerredis) or a user bean,
    resolved exactly like the Redis locker's (they share one
    connection by design). With neither, startup fails fast.
- `redis.key-prefix` — prefix prepended to the cache key when
  `type=redis`. Default `outbox:metrics:`; the cache writes a single
  key `<key-prefix>snapshot`.

A user-defined `@Bean MetricsSnapshotCache` wins over every autowired
variant regardless of `type`.

### `event-outboxer.event-types.defaults` / `event-outboxer.event-types.overrides.<type>`

Per-event-type engine settings. Defaults apply to every type;
per-type overrides adjust individual fields (see
[thin merge](#per-type-override-thin-merge)).

- `poll-min-interval` / `poll-max-interval` / `poll-multiplier` — the
  adaptive poller starts at the min interval, multiplies the wait by
  `poll-multiplier` after every empty poll, and caps it at the max
  interval; any non-empty poll resets the wait to the minimum. Every
  emitted wait additionally carries a uniform ±10% jitter (not
  configurable) so that a fleet of JVMs deployed together does not
  poll the store in lockstep.
  Note: these intervals bound the pickup latency only for events
  published by *other* JVMs and for delayed events (`runAt` in the
  future). Events published in this JVM wake their poller right after
  the publishing transaction commits, so same-JVM latency is
  milliseconds regardless of the poll intervals (ADR-0006 amendment).
- `claim-batch-size` — how many events to claim per poll.
- `handler-pool-size`, `handler-queue-capacity` — fixed-size
  executor per event type (`core == max`, no scaling). Their sum is the
  type's **in-flight budget**: the poller claims at most
  `min(claim-batch-size, free capacity)` per poll and stops claiming
  entirely while the budget is exhausted, resuming the moment a handler
  slot frees. A full claimed batch triggers an immediate re-poll, so
  sustained throughput is bounded by the pool and the database — not by
  `claim-batch-size / poll-min-interval`. For
  `handler-executor.type: virtual` the same sum acts as a soft
  in-flight cap (the executor itself is unbounded). A zero
  `handler-queue-capacity` makes dispatch a synchronous handoff. A
  rejected dispatch (rare capacity race) is not lost: the event is
  released back to `PENDING` (without consuming an attempt) and retried
  after `dispatcher.dispatch-rejected-retry-delay`.
- `handler-max-runtime` — watchdog threshold. A handler running longer
  is force-reclaimed (see ADR-0005).
- `interrupt-stuck-handler` — whether the watchdog also interrupts the
  handler thread it just force-reclaimed (default `true`). The
  reclaimed row is going to reject that handler's finalize anyway
  (ADR-0014), so letting it run only burns a pool slot; the interrupt
  gives the slot back as soon as the handler unwinds. Set to `false`
  for handlers that are not interrupt-safe — for example long database
  work where you would rather rely on `statement_timeout`, since
  `pgjdbc` may close a connection interrupted mid-I/O. Either way, a
  dispatch still running `maintenance.abandoned-handler-grace` after the
  reclaim is reported as abandoned; the callback's `interrupted` flag
  (and the log level) says whether the thread ignored an interrupt or
  was never asked to stop.
- `failure.*` — the retry policy of the type (ADR-0007, ADR-0030):
  `strategy` (`exponential` — the library default — / `fixed` / `none`
  = disable on the first failure), `max-attempts` + `exhausted-action`
  (`DISABLE` / `DELETE`), the exponential knobs `base-delay`,
  `multiplier`, `max-delay`, `jitter`, the `fixed-delay` of the fixed
  strategy, and `log-level` (`OFF` drops the per-failure log line).
  Every key merges independently: a per-type `failure.max-attempts: 5`
  keeps the exponential delays of `defaults.failure` (or of the library
  chain when nothing is set there). Java beans take precedence — see
  [Failure handling](#failure-handling).
- `lock-ttl` — entity-lock TTL passed to `EntityLocker.tryLock()`.
  **Must be `>= handler-max-runtime`** (validated at startup): for
  TTL-honouring lockers (Redis, the PG lease locker) a shorter TTL
  would let the lock expire while a legitimate handler still runs,
  breaking per-key serialization. Default 10m = 2 × the default
  handler budget — keep the 2× margin (the TTL is the crash-release
  mechanism, and the margin covers a zombie handler that outlives its
  force-reclaimed claim; for the lease locker it additionally absorbs
  JVM-vs-DB clock divergence, see ADR-0022 §Clock model). Raising
  `handler-max-runtime` above `lock-ttl` fails startup until
  `lock-ttl` is raised too. See the ADR-0012 amendment for the
  per-backend guarantee table; note that `lock.type=postgres-advisory`
  ignores the TTL and holds one pooled connection per held lock — the
  starter warns when `Σ handler-pool-size >=
  spring.datasource.hikari.maximum-pool-size` (self-deadlock risk;
  does not apply to the default lease locker).

### `event-outboxer.dispatcher.*`

Cross-type dispatcher knobs.

- `unknown-handler-policy` — what to do with a claimed event whose type
  has no registered handler (see ADR-0013): `SKIP` (default —
  reschedule after `unknown-handler-retry-delay` without consuming an
  attempt), `DISABLE` (move to `DISABLED`), `FAIL` (leave the row
  `PROCESSING` as a visible poison-pill marker; it is released back to
  `PENDING` on engine shutdown).
- `unknown-handler-retry-delay` — reschedule delay for `SKIP`.
- `lock-busy-retry-delay` — reschedule delay when the entity lock is
  busy or errored. Lock contention does not consume the attempts
  budget. With the lease locker (`lock.type=postgres-lease`), expect a burst
  of busy-retries after a JVM crash: orphan recovery returns the dead
  worker's events after ~`dead-threshold` (30s), but the dead holder's
  lease blocks the key until `lock-ttl` expires — at the default 1s
  delay that is up to hundreds of claim → busy → release cycles and
  `onLockAcquisitionFailed` emissions per blocked key. Expected
  behaviour, not an incident; raise this delay to shrink the volume
  (see ADR-0022 §Consequences).
- `dispatch-rejected-retry-delay` — reschedule delay when the per-type
  handler executor rejects a dispatch (pool and queue saturated).
  Backpressure does not consume the attempts budget either.
- `finalize-batching` — group-commit batching of `markProcessed` /
  `markForRetry` statements (ADR-0014, batch form): concurrent
  finalizations coalesce into one multi-row statement, cutting finalize
  round-trips on hot types up to ~batch-size×. The batch forms while
  the previous statement is in flight — no timers, no added latency; an
  idle engine degrades to plain single-row calls. `true` by default;
  disable only as a kill-switch.
- `finalize-batch-max-size` — cap on rows per flushed finalize
  statement (default 128).

### `event-outboxer.maintenance.*`

Maintenance-process parameters.

- `heartbeat-interval` — how often the worker refreshes its
  `event_outboxer.workers` row. The PostgreSQL adapter stamps the
  database clock (`now()`), so worker liveness is immune to
  application-JVM clock skew.
- `dead-threshold` — heartbeat silence before a worker is considered
  dead. **Invariant**: `dead-threshold >= 3 × heartbeat-interval`
  (protection against GC-stall false positives).
- `orphan-recovery-interval` — period of `OrphanRecoveryTask`.
- `watchdog-interval` — period of `WatchdogTask` (also used by the
  engine crash detector).
- `abandoned-handler-grace` — how long a force-reclaimed dispatch may
  keep running before its thread counts as lost (default 30s). Past it
  the watchdog fires `OutboxListener.onHandlerAbandoned` once for that
  dispatch and keeps counting the thread in `handler.abandoned_threads`
  until it finally returns. Logged at ERROR when the handler ignored the
  interrupt, at WARN when the type opted out of being interrupted at all
  (`interrupted=false` on the callback). Size it above the worst-case
  unwind of an interrupt-honouring handler so the alert means "this
  thread is never coming back", not "it is still tidying up".
- `reclaim-batch-size` — maximum number of dead workers processed per
  orphan-recovery pass.
- `shutdown-timeout` — maximum wait for in-flight handlers during
  graceful shutdown. Events still claimed after the drain (queued or
  interrupted) are released back to `PENDING` before the worker
  deregisters. See
  [docs/ARCHITECTURE.md §SmartLifecycle phases](ARCHITECTURE.md#3-smartlifecycle-phases)
  for the drain sequence.
- `stale-claim-threshold` — age of a `PROCESSING` claim before the
  stale-claim sweeper returns it to `PENDING` (last line of defence
  for rows invisible to the watchdog and orphan recovery). Default:
  derived as 2 × the largest per-type `handler-max-runtime`. An
  explicit value must exceed every `handler-max-runtime` — validated
  at startup. Heterogeneous fleets (a rolling deploy raising
  `handler-max-runtime`) should set it explicitly with headroom.
- `stale-claim-sweep-interval` — cadence of the sweeper (default 5m).

Note on `handler-max-runtime` semantics: since the in-flight bracket
covers the whole dispatch, the budget includes payload deserialization
and entity-lock acquisition, not just `handler.handle()`.

> See [docs/OBSERVABILITY.md](OBSERVABILITY.md) for what these knobs
> look like from the outside — the health endpoint, the Micrometer
> metric list and seven troubleshooting recipes.

### `event-outboxer.handler-executor.type`

- `platform` (default) — `ThreadPoolTaskExecutor` on platform threads.
- `virtual` — virtual-thread-per-task `ExecutorService` wrapped in
  `ContextPropagatingExecutorService`. Pin-free with
  `synchronized`-heavy JDBC drivers thanks to JEP 491.

### `event-outboxer.metrics.*`

Micrometer listener settings. `MicrometerOutboxListener` registers
automatically when Micrometer and the `event-outboxer-metrics-micrometer`
module are on the classpath and a `MeterRegistry` bean exists.

- `prefix` — prefix applied to every counter / timer / summary
  registered by `MicrometerOutboxListener`. **Default:
  `event_outboxer`** — a specific name chosen to avoid clashing with
  other libraries that publish `outbox.*` metrics. Override when
  multiple outbox instances share a registry or when an organisation
  requires a different namespace. See [docs/OBSERVABILITY.md](OBSERVABILITY.md)
  for the full metric catalogue.

- `distribution-defaults.enabled` — whether the starter applies the SLO
  histogram-bucket defaults shipped with the metrics module
  (`META-INF/event-outboxer/metrics-defaults.yml`) to the
  event-outboxer timers. Applied with the lowest precedence, so any
  `management.metrics.distribution.*` value you set yourself always
  wins. **Default: `true`.**

### `event-outboxer.tracing.*`

Distributed-tracing integration (ADR-0023). A `PRODUCER` span wraps
every event insert and its context is stored with the event; a
`CONSUMER` span restores that context around every handler attempt —
one trace across the outbox hop. The adapter is picked automatically:
`event-outboxer-tracing-micrometer` when Boot's tracing provides
`ObservationRegistry`/`Tracer`/`Propagator` beans, else
`event-outboxer-tracing-otel` when the OpenTelemetry API is present
(OTel Java agent included). The Micrometer adapter instruments through
the Observation API, which has two knock-on effects worth knowing:
it registers four meters under `event-outboxer.metrics.prefix`
(`.publish`, `.publish.active`, `.process`, `.process.active`), and
Boot's `management.observations.enable.*` can no-op it entirely —
spans and stored carrier included — with nothing logged. Remove the
meters with a `MeterFilter`, never with that property. See
[docs/OBSERVABILITY.md §Distributed tracing](OBSERVABILITY.md#distributed-tracing).

- `enabled` — master switch for the adapter auto-detection.
  **Default: `true`.** With no adapter module on the classpath the
  engine uses a zero-cost no-op tracer either way. A user-defined
  `OutboxTracer` bean always wins, regardless of this flag.
- `deferred-propagation` — span shape of a *deferred* event, i.e. one
  published with a `runAt` further ahead than `link-threshold`
  (ADR-0023, 2026-08-28 amendment). `link` (default): the CONSUMER span
  is a new root that carries a span link to the PRODUCER span, and
  both spans are tagged `event_outboxer.propagation=link` — a
  scheduled event does not stretch one trace across the delay, which
  keeps time-range search, tail-based sampling and retention sane.
  `child`: every event keeps parent-child continuity however far
  ahead it is scheduled (the pre-0.4.0 behaviour). The decision is
  taken once, at publish time, from the publisher's intent — backlog
  and retry backoff never change a trace's shape. It is carried in the
  event row's `trace_context` as the extra key
  `event_outboxer.propagation=link`, which the engine strips before
  the carrier reaches a tracing adapter or `EventContext`.
  **Default: `link`.**
- `link-threshold` — how far ahead of the publish-time clock `runAt`
  must lie for the event to count as deferred. `0s` links every event
  with an explicit future `runAt`; immediate publishes (no `runAt`)
  never link. One minute clears every debounce-style `runAt` and the
  decision window of tail-based samplers while anything a human would
  call "scheduled" exceeds it. Ignored under
  `deferred-propagation: child`. **Default: `1m`.**
- On the Micrometer adapter the linked shape needs the starter's
  `OutboxReceiverTracingObservationHandler` bean ahead of Boot's own
  receiver handler (registered automatically, order 900 vs Boot's
  1000); the Brave bridge cannot detach a parent, so on Brave deferred
  events stay parent-child and get the link as tags. Custom
  propagation formats (anything but W3C `traceparent`, `b3`,
  `X-B3-*`) produce an unlinked root span.

### `event-outboxer.health.*`

Spring Boot Actuator integration.

- `probe-groups` — list of Actuator health groups into which the
  `outbox` indicator is merged. Typical values: `readiness`,
  `liveness`. **Default: empty** (the indicator lives only at
  `/actuator/health/outbox`; probes are unaffected). When set, an
  `EnvironmentPostProcessor` appends `outbox` to
  `management.endpoint.health.group.<name>.include` for each listed
  group, preserving your existing includes and the default
  `<name>State` contributor. See [docs/OBSERVABILITY.md §Kubernetes probes](OBSERVABILITY.md#kubernetes-probes)
  for the tradeoffs between probe-driven pod lifecycle and
  metric-driven alerting.

### `event-outboxer.retention.*`

Optional cleanup of the archive table and of accumulated `DISABLED`
events (ADR-0019), executed by a maintenance task on the engine's
scheduler. **Both thresholds default to off** — deleting data is never
a surprise default; enable with one line, e.g.
`retention.archive-older-than: 30d`. `disabled-older-than` ages by
`created_at` (the schema does not record the moment of disabling).
Requires the storage adapter's `OutboxAdmin` (wired automatically by
the starter).

### `event-outboxer.admin.rest.*` and the admin modules

Operational surface over the `OutboxAdmin` SPI port (ADR-0019): list
events by status with keyset pagination, look events up in the
archive, re-enable `DISABLED` events (single or bulk, with a fresh
attempts budget), purge old rows. Two interchangeable surfaces, each
activated by adding its module next to the starter:

- **`event-outboxer-admin-actuator`** — Actuator endpoint
  `outboxadmin`. Not exposed by default; expose with
  `management.endpoints.web.exposure.include=outboxadmin` and secure
  it like any other Actuator endpoint. No `event-outboxer.*`
  properties of its own.
- **`event-outboxer-admin-rest`** — REST controller under
  `base-path`. `enabled` defaults to `false`. Every operation requires
  the authority named by `required-authority` on the authenticated
  principal, enforced via `@PreAuthorize` + Spring **method
  security**. Security posture:
  - no Spring Security on the classpath → the API runs unprotected
    (accepted for security-less apps);
  - Spring Security present but `@EnableMethodSecurity` missing →
    **startup fails** with an actionable message, because the
    annotation would otherwise be silently ignored;
    `enforce-authority: false` is the explicit opt-out.

### Testing without a database

In-memory storage exists solely as test infrastructure and is
unreachable through `event-outboxer.*` properties (ADR-0020). In
Spring tests, opt in explicitly:

```java
@SpringBootTest
@Import(OutboxInMemoryTestConfiguration.class)
class MyOutboxTest { ... }
```

with `event-outboxer-storage-inmemory` on the test classpath:

```xml
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-storage-inmemory</artifactId>
    <scope>test</scope>
</dependency>
```

For plain-Java (non-Spring) tests use the testkit's
`OutboxTestContext`, which wires the in-memory store directly.

### Serialization

The library ships two serializers: Jackson JSON (`jackson-json`, text
lane, ADR-0011) — **included with the starter** and the zero-config
writer — and Protobuf (`protobuf`, bytes lane, ADR-0026), an opt-in
module that activates when it and `protobuf-java` are on the
classpath. Customise Jackson by providing an `ObjectMapper` bean named
`outboxObjectMapper` (falls back to the primary `ObjectMapper`, then
to library defaults). Customise Protobuf by providing an
`ExtensionRegistryLite` bean or your own `ProtobufEventSerializer`
bean; protobuf payloads must be protoc-generated `Message` classes
(schema-first, ADR-0026). Any other format plugs in via your own
`@Bean EventSerializer`.

The serialization seam itself is format-flexible (ADR-0025): every
registered `EventSerializer` bean is available for **deserialization**
— the dispatcher routes by the `payload_format` recorded on each event
at publish time — while exactly one serializer **writes** new events.
The write serializer resolves as:

1. `event-outboxer.serializer.write-format`, when set (startup fails if
   it matches no registered format);
2. the only registered bean — the zero-config default;
3. the bean named `outboxEventSerializer` — the documented override
   wins even next to extra read-only serializers;
4. otherwise startup fails listing the registered formats.

```yaml
event-outboxer:
  serializer:
    write-format: jackson-json    # default writer (only needed with several serializer beans)
    write-format-per-type:        # per-event-type overrides (ADR-0025 amendment)
      ORDER_CREATED: protobuf     # this type writes protobuf; every other type keeps the default
```

With the starter's Jackson serializer and the protobuf module on the
classpath and no `write-format`, Jackson keeps writing (rule 3 — its
auto-configured bean carries the `outboxEventSerializer` name) and
`protobuf` registers read-only; set `write-format: protobuf` to switch
the writer — the recommended protobuf setup, since Jackson stays
read-only for in-flight JSON events. For a protobuf-only classpath
exclude the Jackson module from the starter; the single bean then
writes with zero config (rule 2):

```xml
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-spring-boot-starter</artifactId>
    <exclusions>
        <exclusion>
            <groupId>io.github.bams22</groupId>
            <artifactId>event-outboxer-serializer-jackson</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

Excluding it without another serializer fails startup with a
`FailureAnalyzer` diagnosis (`NoEventSerializersException`) naming the
exclusion, the protobuf module and `write-format`.

`write-format-per-type` maps event types to the format that writes
them instead of the default — each listed format must belong to a
registered serializer bean (startup fails fast otherwise, listing the
registered formats). Deserialization is unaffected: reads always route
by the `payload_format` stored on each event. Plain-Java setups get
the same knob via `OutboxEngineBuilder.writeSerializerOverride(type,
serializer)` (the override serializer is auto-registered for reads),
and the testkit mirrors it on `OutboxTestContext.Builder`.

#### Migrating between payload formats

Because reads route by the stored format, a format migration (e.g.
`jackson-json` → `protobuf`) needs no data rewrite:

1. Register the new serializer bean alongside the old one; keep the old
   format writing (or set `write-format` to the old id).
2. Deploy everywhere — every replica can now read both formats.
3. Switch `write-format` to the new id and deploy again. In-flight
   events written in the old format keep deserializing with the old
   serializer until they drain.
4. Once no old-format events remain (check `payload_format` in the
   events/archive tables), drop the old serializer bean.

For a **gradual migration one event type at a time**, use
`write-format-per-type` instead of flipping the global writer at once:
move a single type to the new format, watch it drain and process
cleanly, then add the next type. Once every type is listed (or you are
confident), switch `write-format` to the new id and drop the map.

An event whose stored format has no registered serializer is not lost:
`OUTBOX-203` routes through the `FailureHandler` chain (retry with
backoff), so a replica that knows the format can pick it up later.

#### DTO evolution and rolling deploys

The library's default `ObjectMapper` is deliberately
evolution-friendly (`FAIL_ON_UNKNOWN_PROPERTIES` disabled): during a
rolling deploy, mixed-version replicas read each other's payloads, and
strictness there would disable events instead of processing them.
With the defaults:

- **Adding a DTO field is safe in both directions.** An outdated
  replica ignores the unknown field; an updated replica reads an old
  payload with the field absent (defaults / `null` — give new fields
  a default value or a nullable type).
- **Removing a field is safe** — old payloads simply carry an ignored
  extra property.
- **Renaming** — use `@JsonAlias("oldName")` on the new component for
  one release, then drop it once pre-rename events are drained.
- **Type changes are not safe**; publish a new event type instead.

If deserialization does fail (a truly incompatible change or corrupt
data), the event is **not** lost: the failure routes through the
`FailureHandler` chain — retried with backoff, `DISABLED` only after
the chain's attempt budget (10 by default) is exhausted, with
`OutboxListener.onEventSerializationError` fired on every failed
attempt. Strict deserialization can be restored by supplying a strict
`@Bean("outboxObjectMapper")`.

### Failure handling

The retry/backoff policy has three sources (ADR-0007, ADR-0030):

- **YAML** — `event-outboxer.event-types.defaults.failure.*` and
  `event-outboxer.event-types.overrides.<TYPE>.failure.*`, thin-merged
  key by key onto the library chain `FailureHandlers.defaults()`
  (logging at WARN → max-attempts 10, then DISABLE → exponential
  backoff 5s ×2 capped at 1h, jitter 0.2). Bad values fail startup
  naming the property (see [Invariant validation](#invariant-validation)).
- **Java beans** — a `FailureHandler` bean annotated
  `@OutboxFailureHandler` (global) or `@OutboxFailureHandler({"A", "B"})`
  (per type); see
  [Custom FailureHandler](#custom-failurehandler-global-or-per-type).
  The pre-ADR-0030 forms — a bean named `outboxDefaultFailureHandler`
  and a `Map<String, FailureHandler<?>>` bean named
  `outboxPerTypeFailureHandlers` — keep working. Two beans claiming the
  same slot fail startup with a diagnosis; a `FailureHandler` bean that
  claims no slot is **not** used and is listed in a startup WARN.
- **The handler itself** — `EventHandler.failureHandler()`.

Precedence — the most specific source wins, and Java beats YAML at
equal specificity:

1. `EventHandler.failureHandler()`;
2. a per-type bean;
3. `overrides.<TYPE>.failure.*`;
4. the global bean;
5. `defaults.failure.*`;
6. `FailureHandlers.defaults()`.

A per-type YAML override always builds its full chain from YAML layers
(override → `defaults.failure` → library) — never from a global Java
bean, which is opaque to the merge.

---

## Per-type override (thin merge)

`event-outboxer.event-types.overrides.<type>` overrides
`event-outboxer.event-types.defaults` **field by field,
independently**. Unset fields in `defaults` fall back to the library
defaults (`EventTypeConfig.defaults()`). For example:

```yaml
event-outboxer:
  event-types:
    defaults:
      poll-min-interval: 250ms
      claim-batch-size: 42
    overrides:
      SEND_EMAIL:
        handler-pool-size: 20   # ONLY this is overridden
```

Effective configuration for `SEND_EMAIL`:
- `handler-pool-size: 20` (override)
- `poll-min-interval: 250ms` (from defaults)
- `claim-batch-size: 42` (from defaults)
- everything else — library defaults.

The merge is performed by the starter when it maps `OutboxProperties`
to the core `EventTypeConfig` objects. The nested `failure.*` group
merges the same way, independently of the other fields: an override
that sets only `failure.max-attempts` keeps every other retry knob of
`defaults.failure` (or of the library chain).

---

## Invariant validation

Violations fail fast at startup — the configuration records validate
their invariants in their constructors, so a bad value aborts context
refresh:

| Rule | Where | Why |
|---|---|---|
| `dead-threshold >= 3 × heartbeat-interval` | `MaintenanceConfig` | Protect against GC-stall false positives |
| `poll-min-interval > 0`, `poll-max-interval >= poll-min-interval` | `EventTypeConfig` | Adaptive backoff needs a sane range |
| `poll-multiplier > 1.0` | `EventTypeConfig` | Adaptive backoff needs growth |
| `claim-batch-size > 0`, `handler-pool-size > 0`, `handler-queue-capacity >= 0` | `EventTypeConfig` | Pool is fixed-size and bounded |
| `handler-max-runtime > 0`, `lock-ttl > 0` | `EventTypeConfig` | Sanity |
| `abandoned-handler-grace >= 0` | `MaintenanceConfig` | Sanity |
| retry delays not negative | `DispatcherConfig` | Sanity |
| `failure.max-attempts >= 1`, `failure.base-delay > 0`, `failure.multiplier > 1.0`, `failure.max-delay > 0`, `failure.jitter` in `[0, 1]`, `failure.fixed-delay > 0` | `FailurePolicyFactory` (starter) | Each checked on the layer that sets it — the error names the exact property, e.g. `event-outboxer.event-types.overrides.SEND_EMAIL.failure.multiplier must be > 1.0` |
| `failure.max-delay >= failure.base-delay` | `FailurePolicyFactory` (starter) | Checked on the merged policy; the error names both keys |

---

## Overriding through Java code

Property binding is not enough for complex cases. Override via Spring
beans:

### Selecting the DataSource (`@OutboxDataSource`)

With a single `DataSource` bean nothing is needed — the starter uses
it. With several, mark the one whose database holds the outbox tables
(ADR-0024), mirroring Spring Boot's own `@FlywayDataSource` /
`@BatchDataSource` pattern:

```java
@Bean
@OutboxDataSource
public DataSource ordersDataSource() { ... }   // outbox lives here

@Bean
public DataSource reportingDataSource() { ... }
```

Resolution order — applied identically by the PostgreSQL storage
adapter, both PostgreSQL lockers and the lease-table probe (they share
one database by design):

1. the single bean marked
   `@io.github.bams22.outboxer.spring.OutboxDataSource` — wins even
   when another bean is `@Primary`;
2. otherwise the unique `DataSource` bean, or the `@Primary` one among
   several;
3. otherwise startup fails fast, naming the candidate beans and the
   fix. Two beans carrying the qualifier fail the same way — exactly
   one may.

Notes:

- The storage adapter wraps the resolved `DataSource` in
  `TransactionAwareDataSourceProxy` as always (ADR-0002); the PG
  lockers unwrap such a proxy back to its raw target — their
  acquire/release statements must run autocommit, never inside the
  caller's transaction (ADR-0022).
- Only Java-config beans can carry the qualifier. A `DataSource`
  existing purely through `spring.datasource.*` properties
  participates via the primary/unique rule instead.
- User-defined `ConnectionSupplier` / `EntityLocker` beans still
  override the outbox JDBC wiring entirely.

### Selecting the Redis connection (`@OutboxRedisConnection`)

With [`event-outboxer.redis.*`](#event-outboxerredis) set or a single
`StatefulRedisConnection<String, String>` bean, nothing is needed.
With several connection beans, mark the one the outbox should use
(ADR-0027), mirroring `@OutboxDataSource`:

```java
@Bean(destroyMethod = "close")
@OutboxRedisConnection
public StatefulRedisConnection<String, String> outboxRedis(RedisClient client) {
    return client.connect();
}
```

Resolution order — applied identically by the Redis locker and the
Redis metrics cache (they share one connection by design):

1. the single bean marked
   `@io.github.bams22.outboxer.spring.OutboxRedisConnection` — wins
   even when another bean is `@Primary`;
2. otherwise the unique `StatefulRedisConnection` bean, or the
   `@Primary` one among several;
3. otherwise startup fails fast, naming the candidate beans and the
   fix. Two beans carrying the qualifier fail the same way — exactly
   one may.

Notes:

- Any user-defined connection bean disables the starter-managed
  connection entirely — `event-outboxer.redis.*` becomes inert.
- The starter-created connection (bean name `outboxRedisConnection`)
  carries the qualifier itself, so it resolves deterministically even
  next to unrelated user connections.
- User-defined `EntityLocker` / `MetricsSnapshotCache` beans still
  override the outbox Redis wiring entirely.

### Custom ObjectMapper for serialization

```java
@Bean
public ObjectMapper outboxObjectMapper() {
    return JsonMapper.builder()
        .addModule(new JavaTimeModule())
        .addModule(new KotlinModule.Builder().build())
        .build();
}
```

### Custom TaskDecorator for context propagation

```java
@Bean
public TaskDecorator myOutboxTaskDecorator() {
    return runnable -> {
        String tenantId = TenantContext.current();
        return () -> {
            TenantContext.set(tenantId);
            try { runnable.run(); }
            finally { TenantContext.clear(); }
        };
    };
}
```

### Custom FailureHandler (global or per type)

Register the chain with `@OutboxFailureHandler` (ADR-0030). The
builder's terminators (`withExponentialBackoff`, `withFixedDelay`,
`withNoRetry`) return the chain — there is no `build()`:

```java
import org.slf4j.event.Level;
import io.github.bams22.outboxer.api.handle.builtin.MaxRetriesFailureHandler.ExhaustedAction;

@Bean
@OutboxFailureHandler                       // global chain (precedence 4)
FailureHandler<Object> outboxFailures() {
    return FailureHandlers.builder()
        .withLogging(Level.WARN)
        .withMaxAttempts(5, ExhaustedAction.DISABLE)
        .withExponentialBackoff(
            Duration.ofSeconds(30), 2.0, Duration.ofHours(2), 0.2);
}

@Bean
@OutboxFailureHandler({"SEND_EMAIL", "SEND_SMS"})   // per-type chain (precedence 2)
FailureHandler<Object> notificationFailures() {
    return FailureHandlers.builder()
        .withMaxAttempts(20, ExhaustedAction.DELETE)
        .withFixedDelay(Duration.ofMinutes(1));
}
```

Exactly one bean may claim a slot (the global chain, or one event
type); a second claim fails startup naming both beans. The legacy
forms still work: a bean named `outboxDefaultFailureHandler` for the
global chain and a `@Bean("outboxPerTypeFailureHandlers")
Map<String, FailureHandler<?>>` for per-type chains. A `FailureHandler`
bean without the annotation or a legacy name is not registered — the
starter logs a WARN listing such beans (ignore it when the bean is
returned from `EventHandler.failureHandler()`).

### Custom OutboxListener

```java
@Component
public class AuditListener implements OutboxListener {
    private final AuditLogRepository repo;

    @Override
    public void onEventDisabled(EventDisabledInfo info) {
        repo.save(new AuditEntry(
            info.eventId(), "DISABLED", info.reason(), now()));
    }
}
```

### Custom EventHandler.failureHandler()

```java
@Component
public class ValidationHandler implements EventHandler<ValidationPayload> {
    public static final EventType<ValidationPayload> VALIDATE =
        EventType.of("VALIDATE", ValidationPayload.class);

    @Override
    public EventType<ValidationPayload> type() {
        return VALIDATE;
    }

    @Override
    public FailureHandler<ValidationPayload> failureHandler() {
        // validation is not retried
        return new NoRetryFailureHandler<>();
    }
    // ...
}
```

This level wins over every bean and every YAML key (precedence 1).

### Custom OutboxTracer

A user-defined bean replaces both tracing auto-configurations
(ADR-0023) — for a bespoke backend or non-standard span naming:

```java
@Bean
OutboxTracer outboxTracer(OpenTelemetry otel) {
    return new OtelOutboxTracer(otel); // or your own implementation
}
```

---

## Related documents

- [docs/ARCHITECTURE.md](ARCHITECTURE.md) — architectural overview.
- [docs/adr/README.md](adr/README.md) — rationale.
