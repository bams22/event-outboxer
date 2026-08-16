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

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;

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

    private final Storage storage = new Storage();
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
        private @Nullable Integer handlerPoolSize;
        private @Nullable Integer handlerQueueCapacity;
        private @Nullable Duration handlerMaxRuntime;

        /**
         * Whether the watchdog interrupts the handler thread after force-reclaiming its row.
         * Defaults to {@code true}; set to {@code false} for handlers that are not interrupt-safe.
         */
        private @Nullable Boolean interruptStuckHandler;

        private @Nullable Duration lockTtl;
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
