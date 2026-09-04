/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring;

import io.github.bams22.outboxer.api.handle.builtin.MaxRetriesFailureHandler;
import io.github.bams22.outboxer.spi.OutboxTracer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.logging.LogLevel;

/**
 * Root {@code @ConfigurationProperties} for {@code event-outboxer.*}. See CONFIGURATION.md for the
 * documented YAML shape.
 *
 * <p>Defaults match the per-class library defaults ({@code EventTypeConfig.defaults()}, {@code
 * MaintenanceConfig.defaults()}, {@code DispatcherConfig.defaults()}) so that a user who only sets
 * {@code spring.datasource.url} gets a working outbox out of the box.
 */
@Getter
@Setter
@ConfigurationProperties("event-outboxer")
public class OutboxProperties {

    /** Master switch; set to {@code false} to disable all auto-configuration. */
    private boolean enabled = true;

    /**
     * Declares this application instance publish-only (ADR-0029): the engine registers its worker,
     * runs maintenance and exposes {@code OutboxEventPublisher}, but starts no pollers — any {@code
     * EventHandler} beans are ignored. Default {@code false}: at least one {@code EventHandler}
     * bean is then required and startup fails with an actionable diagnosis without one.
     */
    private boolean publishOnly = false;

    private final Storage storage = new Storage();
    private final Flyway flyway = new Flyway();
    private final Redis redis = new Redis();
    private final Lock lock = new Lock();
    private final Publisher publisher = new Publisher();
    private final Serializer serializer = new Serializer();
    private final Maintenance maintenance = new Maintenance();
    private final Dispatcher dispatcher = new Dispatcher();
    private final EventTypes eventTypes = new EventTypes();
    private final Worker worker = new Worker();
    private final HandlerExecutor handlerExecutor = new HandlerExecutor();
    private final Metrics metrics = new Metrics();
    private final Tracing tracing = new Tracing();
    private final Health health = new Health();
    private final Cache cache = new Cache();
    private final Retention retention = new Retention();

    // =============================================================================================
    // nested groups
    // =============================================================================================

    @Getter
    @Setter
    public static class Storage {
        /**
         * Storage adapter. REQUIRED for production — there is no default and no in-memory option
         * (ADR-0020): a silently non-durable outbox would betray the library's whole contract.
         * Tests without a database import {@code OutboxInMemoryTestConfiguration} explicitly
         * instead of configuring a type.
         */
        private @Nullable StorageType type;

        /**
         * Schema name for the PG adapter. Default: {@code event_outboxer} — a specific name chosen
         * to avoid clashing with other libraries or application tables in a shared database.
         * Propagated both into the adapter's SQL (via {@code SchemaResolver}) and into the Flyway
         * {@code ${eventOutboxerSchema}} placeholder used by the classpath migrations.
         */
        private String schema = "event_outboxer";

        /** Optional table prefix; default empty. */
        private String tablePrefix = "";

        /** Copy successful events to the archive table before deleting. */
        private boolean archiveEnabled = false;

        /** TTL of the in-memory cache for {@code metricsSnapshot()}. */
        private Duration metricsCacheTtl = Duration.ofSeconds(30);
    }

    public enum StorageType {
        postgres
    }

    /**
     * The starter-managed Flyway instance that applies the library's own schema migrations
     * (ADR-0028). It is independent of the application's {@code spring.flyway.*} instance: its
     * locations are fixed to the migrations shipped on the classpath, its history table lives
     * inside {@code event-outboxer.storage.schema}, and it runs against the outbox {@code
     * DataSource} unless {@code url} (and optionally {@code user} / {@code password}) point it at a
     * dedicated connection — for example a role that owns DDL rights while the application role
     * does not.
     */
    @Getter
    @Setter
    public static class Flyway {
        /**
         * Whether the starter runs the outbox migrations itself. Default {@code true} whenever
         * Flyway is on the classpath; set to {@code false} to apply the shipped SQL through your
         * own Flyway / Liquibase pipeline or by hand.
         */
        private boolean enabled = true;

        /**
         * JDBC URL of a dedicated migration connection. {@code null} (default): migrate through the
         * outbox {@code DataSource} (the {@code @OutboxDataSource}-qualified bean, else the unique
         * / {@code @Primary} one — ADR-0024).
         */
        private @Nullable String url;

        /**
         * Login user of the migration connection. With {@code url} unset, a non-null value derives
         * a connection from the outbox {@code DataSource} with these credentials.
         */
        private @Nullable String user;

        /** Login password of the migration connection. */
        private @Nullable String password;

        /**
         * Fully qualified JDBC driver class for {@code url}; {@code null} = detected from the URL.
         */
        private @Nullable String driverClassName;

        /**
         * Flyway {@code baselineOnMigrate}: when the outbox schema already holds tables but no
         * history table yet, record a baseline at {@link #baselineVersion} instead of failing.
         * Needed once when upgrading from a release that applied the outbox migrations through the
         * application's Flyway instance (see CHANGELOG / ADR-0028).
         */
        private boolean baselineOnMigrate = false;

        /**
         * Version recorded by the baseline; migrations at or below it are skipped. {@code 7} marks
         * every migration shipped up to 0.4.0 as applied.
         */
        private String baselineVersion = "1";
    }

    /**
     * Connection details for the starter-managed Lettuce connection (ADR-0027). When {@code uri} or
     * {@code host} is set, the starter creates and owns a {@code StatefulRedisConnection} shared by
     * the Redis entity locker and the Redis metrics cache. Ignored entirely when the application
     * defines its own {@code StatefulRedisConnection} bean.
     */
    @Getter
    @Setter
    public static class Redis {
        /**
         * Full Lettuce {@code RedisURI}, e.g. {@code redis://localhost:6379/0} or {@code
         * redis-sentinel://host1,host2/0#mymaster}. Wins over the discrete fields when set.
         */
        private @Nullable String uri;

        /** Redis host. Used only when {@code uri} is not set. */
        private @Nullable String host;

        /** Redis port. */
        private int port = 6379;

        /** Username for Redis ACL authentication; requires {@code password}. */
        private @Nullable String username;

        /** Password for Redis authentication. */
        private @Nullable String password;

        /** Database index. */
        private int database = 0;

        /** Connect over SSL/TLS. */
        private boolean ssl = false;

        /**
         * Connect and command timeout. {@code null} (default): Lettuce's own default (60 seconds).
         * Also bounds how long a down Redis can block application startup.
         */
        private @Nullable Duration timeout;

        /** Client name reported to the server ({@code CLIENT SETNAME}). */
        private @Nullable String clientName;
    }

    @Getter
    @Setter
    public static class Lock {
        /**
         * {@code noop}, {@code postgres-lease} (lease table, ADR-0022), {@code postgres-advisory}
         * (session-scoped advisory locks, pre-ADR-0022 behaviour) or {@code redis}. The
         * pre-ADR-0022 value {@code postgres} no longer binds — startup fails listing the valid
         * values, forcing an explicit choice between the two PostgreSQL backends.
         */
        private LockType type = LockType.noop;

        private String keyPrefix = "outbox:lock:";
    }

    /**
     * Entity-locker backend. {@code postgres_lease} / {@code postgres_advisory} bind from {@code
     * postgres-lease} / {@code postgres-advisory} via relaxed binding.
     */
    public enum LockType {
        noop,
        postgres_lease,
        postgres_advisory,
        redis
    }

    @Getter
    @Setter
    public static class Cache {
        /**
         * Selects the backing store for {@code MetricsSnapshotCache}. {@code memory} (default)
         * keeps the per-JVM TTL cache from pre-SPI behaviour; {@code redis} uses the {@code
         * event-outboxer-cache-redis} module to share a single snapshot across pods; {@code noop}
         * disables caching entirely so each {@code metricsSnapshot()} call recomputes from the
         * database.
         */
        private CacheType type = CacheType.memory;

        private final Redis redis = new Redis();

        @Getter
        @Setter
        public static class Redis {
            /** Prefix prepended to the cache key. */
            private String keyPrefix = "outbox:metrics:";
        }
    }

    public enum CacheType {
        memory,
        redis,
        noop
    }

    @Getter
    @Setter
    public static class Publisher {
        /** {@code FAIL} or {@code IGNORE}. Default: {@code FAIL}. */
        private NoTxPolicy noTransactionPolicy = NoTxPolicy.FAIL;
    }

    public enum NoTxPolicy {
        FAIL,
        IGNORE
    }

    /**
     * Serialization (ADR-0025). All registered {@code EventSerializer} beans are available for
     * deserialization (routed by the {@code payload_format} stored per event); exactly one of them
     * writes new events.
     */
    @Getter
    @Setter
    public static class Serializer {
        /**
         * Format id of the serializer that writes new events, e.g. {@code jackson-json}. Only
         * needed when more than one {@code EventSerializer} bean is registered and none of them is
         * the documented {@code outboxEventSerializer} override; with a single bean it is
         * redundant. {@code null} (default): resolve automatically.
         */
        private @Nullable String writeFormat;

        /**
         * Per-event-type write serializer overrides: event type → format id, e.g. {@code
         * write-format-per-type.ORDER_CREATED: protobuf}. Events of a listed type are written with
         * that format; every other type keeps the default writer. Each format must belong to a
         * registered {@code EventSerializer} bean — startup fails fast otherwise. Useful for a
         * gradual format migration one event type at a time (ADR-0025 amendment).
         */
        private Map<String, String> writeFormatPerType = new LinkedHashMap<>();
    }

    /**
     * Retention of the archive table and of accumulated {@code DISABLED} events (ADR-0019). Both
     * thresholds default to {@code null} = off — deleting data is never a surprise default.
     */
    @Getter
    @Setter
    public static class Retention {
        /** Delete archive rows older than this. {@code null} (default): archive retention off. */
        private @Nullable Duration archiveOlderThan;

        /**
         * Delete {@code DISABLED} events created earlier than this ago (age approximated by {@code
         * created_at}). {@code null} (default): off.
         */
        private @Nullable Duration disabledOlderThan;

        /** Rows deleted per statement; a pass loops until a batch comes back short. */
        private int batchSize = 1000;

        /** Delay between retention passes. */
        private Duration interval = Duration.ofHours(1);
    }

    @Getter
    @Setter
    public static class Maintenance {
        private Duration heartbeatInterval = Duration.ofSeconds(5);
        private Duration deadThreshold = Duration.ofSeconds(30);
        private Duration orphanRecoveryInterval = Duration.ofSeconds(30);
        private Duration watchdogInterval = Duration.ofSeconds(10);

        /**
         * How long a force-reclaimed dispatch may keep running before the watchdog reports its
         * thread as lost to the handler pool ({@code handler.abandoned}). Covers the unwind of a
         * handler that honours the interrupt.
         */
        private Duration abandonedHandlerGrace = Duration.ofSeconds(30);

        private int reclaimBatchSize = 50;
        private Duration shutdownTimeout = Duration.ofSeconds(30);

        /**
         * Age of a PROCESSING claim before the stale-claim sweeper returns it to PENDING. Default
         * {@code null} = derived as 2 × the largest per-type handler-max-runtime. An explicit value
         * must exceed every handler-max-runtime (validated at startup).
         */
        private @Nullable Duration staleClaimThreshold;

        /** Cadence of the stale-claim sweeper. */
        private Duration staleClaimSweepInterval = Duration.ofMinutes(5);
    }

    @Getter
    @Setter
    public static class Dispatcher {
        /** {@code SKIP}, {@code DISABLE} or {@code FAIL} for events with no registered handler. */
        private UnknownPolicy unknownHandlerPolicy = UnknownPolicy.SKIP;

        private Duration unknownHandlerRetryDelay = Duration.ofMinutes(1);
        private Duration lockBusyRetryDelay = Duration.ofSeconds(1);
        private Duration dispatchRejectedRetryDelay = Duration.ofSeconds(1);

        /**
         * Group-commit batching of finalize statements ({@code markProcessed} / {@code
         * markForRetry}): concurrent finalizations coalesce into one multi-row statement. Degrades
         * to plain single-row calls on an idle engine; disable only as a kill-switch.
         */
        private boolean finalizeBatching = true;

        /** Cap on rows per flushed finalize statement when {@code finalizeBatching} is on. */
        private int finalizeBatchMaxSize = 128;
    }

    public enum UnknownPolicy {
        SKIP,
        DISABLE,
        FAIL
    }

    @Getter
    @Setter
    public static class EventTypes {
        private EventType defaults = new EventType();

        /**
         * Per-type overrides keyed by event type. Thin merge: each entry overrides only the fields
         * it sets explicitly; every unset field falls back to {@code defaults} (which in turn falls
         * back to the library defaults).
         */
        private Map<String, EventType> overrides = new LinkedHashMap<>();
    }

    /**
     * Per-event-type knobs. All fields are nullable on purpose: {@code null} means "not set here",
     * which is what makes the thin merge in {@code OutboxEngineAutoConfiguration} possible — a
     * populated default would be indistinguishable from an explicit user value. Library defaults
     * live in {@code EventTypeConfig.defaults()} (core).
     */
    @Getter
    @Setter
    public static class EventType {
        private @Nullable Duration pollMinInterval;
        private @Nullable Duration pollMaxInterval;
        private @Nullable Double pollMultiplier;
        private @Nullable Integer claimBatchSize;

        /**
         * Free in-flight capacity the poller waits for before it claims again (low-watermark
         * refill). Library default {@code 1}: claim as soon as one slot frees. Raise it to refill
         * the handler queue in bulk; must stay within {@code [1, handler-pool-size +
         * handler-queue-capacity]}.
         */
        private @Nullable Integer claimMinFree;

        private @Nullable Integer handlerPoolSize;
        private @Nullable Integer handlerQueueCapacity;
        private @Nullable Duration handlerMaxRuntime;

        /**
         * Whether the watchdog interrupts the handler thread after force-reclaiming its row.
         * Defaults to {@code true}; set to {@code false} for handlers that are not interrupt-safe.
         */
        private @Nullable Boolean interruptStuckHandler;

        private @Nullable Duration lockTtl;

        /**
         * Bounded wait for a busy entity lock (ADR-0035): how long the handler thread keeps
         * retrying the lock before the event is released with {@code
         * dispatcher.lock-busy-retry-delay}. Library default {@code 100ms}; {@code 0} = one
         * non-blocking attempt. Must be {@code < handler-max-runtime}.
         */
        private @Nullable Duration lockWait;

        /**
         * Retry / failure policy for this type (ADR-0030). Thin merge like the fields above: a
         * per-type entry overrides only the {@code failure.*} keys it sets, everything else comes
         * from {@code defaults.failure.*} and then from the library chain {@code
         * FailureHandlers.defaults()}.
         */
        private final Failure failure = new Failure();
    }

    /**
     * Retry / failure policy knobs (ADR-0007, ADR-0030). All fields nullable: {@code null} means
     * "not set at this level". With every field unset at the {@code defaults} level the starter
     * leaves the engine's library chain untouched. Java beans ({@code @OutboxFailureHandler}) and
     * {@code EventHandler.failureHandler()} take precedence over these properties — see
     * CONFIGURATION.md §Failure handling.
     */
    @Getter
    @Setter
    public static class Failure {
        /**
         * Retry strategy: {@code exponential} (library default), {@code fixed}, or {@code none}
         * (disable on the first failure).
         */
        private @Nullable FailureStrategy strategy;

        /**
         * Attempts before {@code exhausted-action} applies (&gt;= 1). Library default 10; ignored
         * with strategy {@code none}.
         */
        private @Nullable Integer maxAttempts;

        /**
         * {@code DISABLE} (library default) or {@code DELETE} once {@code max-attempts} is
         * exhausted.
         */
        private MaxRetriesFailureHandler.@Nullable ExhaustedAction exhaustedAction;

        /** {@code exponential}: delay before the first retry (&gt; 0). Library default 5s. */
        private @Nullable Duration baseDelay;

        /** {@code exponential}: growth factor per attempt (&gt; 1.0). Library default 2.0. */
        private @Nullable Double multiplier;

        /**
         * {@code exponential}: upper bound of the backoff (&gt;= base-delay). Library default 1h.
         */
        private @Nullable Duration maxDelay;

        /**
         * {@code exponential}: random jitter as a fraction of the delay, in [0, 1]. Library default
         * 0.2.
         */
        private @Nullable Double jitter;

        /** {@code fixed}: constant delay between attempts (&gt; 0). Library default 30s. */
        private @Nullable Duration fixedDelay;

        /**
         * Level of the log line written per failure; {@code OFF} removes the logging decorator.
         * Library default WARN.
         */
        private @Nullable LogLevel logLevel;
    }

    /** Retry strategy of a failure policy — the leaf of the chain built from {@code failure.*}. */
    public enum FailureStrategy {
        /** Exponential backoff with jitter and an upper bound (the library default). */
        exponential,
        /** A constant delay between attempts. */
        fixed,
        /** No retry: the first failure applies {@code exhausted-action} immediately. */
        none
    }

    @Getter
    @Setter
    public static class Worker {
        /** Explicit worker id. When null, the engine generates one. */
        private @Nullable String id;

        /** Explicit hostname. When null, the engine resolves it. */
        private @Nullable String host;

        private Map<String, String> metadata = new LinkedHashMap<>();
    }

    @Getter
    @Setter
    public static class HandlerExecutor {
        /** {@code platform} (default) or {@code virtual} (JDK 21+ virtual threads). */
        private ExecutorType type = ExecutorType.platform;
    }

    public enum ExecutorType {
        platform,
        virtual
    }

    @Getter
    @Setter
    public static class Metrics {
        /**
         * Prefix applied to every Micrometer metric published by the outbox. Default: {@code
         * event_outboxer} — a specific name chosen to avoid clashing with other libraries. Override
         * when multiple outbox instances share a registry or when an organisation requires a
         * different namespace.
         */
        private String prefix = "event_outboxer";
    }

    @Getter
    @Setter
    public static class Tracing {
        /**
         * Master switch for the auto-configured {@code OutboxTracer} adapters (ADR-0023):
         * Micrometer Tracing and OpenTelemetry detection both back off when {@code false}. A
         * user-defined {@code OutboxTracer} bean is always honoured regardless of this flag.
         */
        private boolean enabled = true;

        /**
         * Span shape of a deferred event (ADR-0023, 2026-08-28 amendment). {@code LINK} (default):
         * an event whose {@code runAt} lies further than {@link #linkThreshold} ahead of the
         * publish-time clock gets a CONSUMER span that is a new root <em>linking</em> to the
         * PRODUCER span, so a deliberately scheduled event does not stretch one trace across the
         * delay. {@code CHILD}: every event keeps parent-child continuity however far ahead it is
         * scheduled — the pre-0.4.0 behaviour. Decided once at publish; retries and backlog never
         * revisit it.
         */
        private OutboxTracer.Propagation deferredPropagation = OutboxTracer.Propagation.LINK;

        /**
         * How far ahead of the publish-time clock {@code runAt} must lie for the event to count as
         * deferred under {@link #deferredPropagation}. {@code PT0S} links every event with an
         * explicit future {@code runAt}. Default: 1 minute — beyond any debounce-style {@code
         * runAt} and beyond the decision window of tail-based samplers.
         */
        private Duration linkThreshold = Duration.ofMinutes(1);
    }

    @Getter
    @Setter
    public static class Health {
        /**
         * Actuator health groups into which the {@code outbox} indicator is merged. Typical values:
         * {@code readiness}, {@code liveness}. Default is empty — no influence on any probe; the
         * indicator lives only at {@code /actuator/health/outbox}.
         *
         * <p>When set, an {@code EnvironmentPostProcessor} appends {@code outbox} to {@code
         * management.endpoint.health.group.<name>.include} for each listed group, preserving the
         * user's existing includes and the default {@code <name>State} contributor so Spring Boot's
         * liveness / readiness semantics keep working.
         */
        private List<String> probeGroups = new ArrayList<>();
    }
}
