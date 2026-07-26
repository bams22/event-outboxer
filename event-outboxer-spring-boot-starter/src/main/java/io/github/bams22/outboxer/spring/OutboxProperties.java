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
 * Root {@code @ConfigurationProperties} for {@code event-outboxer.*}. See CONFIGURATION.md for
 * the documented YAML shape.
 *
 * <p>Defaults match the per-class library defaults ({@code EventTypeConfig.defaults()},
 * {@code MaintenanceConfig.defaults()}, {@code DispatcherConfig.defaults()}) so that a user who
 * only sets {@code spring.datasource.url} gets a working outbox out of the box.
 */
@Getter
@Setter
@ConfigurationProperties("event-outboxer")
public class OutboxProperties {

  /** Master switch; set to {@code false} to disable all auto-configuration. */
  private boolean enabled = true;

  private final Storage storage = new Storage();
  private final Lock lock = new Lock();
  private final Publisher publisher = new Publisher();
  private final Maintenance maintenance = new Maintenance();
  private final Dispatcher dispatcher = new Dispatcher();
  private final EventTypes eventTypes = new EventTypes();
  private final Worker worker = new Worker();
  private final HandlerExecutor handlerExecutor = new HandlerExecutor();
  private final Metrics metrics = new Metrics();
  private final Health health = new Health();
  private final Cache cache = new Cache();

  // =============================================================================================
  // nested groups
  // =============================================================================================

  @Getter
  @Setter
  public static class Storage {
    /** {@code inmemory} or {@code postgres}. Default: {@code inmemory}. */
    private StorageType type = StorageType.inmemory;

    /**
     * Schema name for the PG adapter. Default: {@code event_outboxer} — a
     * specific name chosen to avoid clashing with other libraries or
     * application tables in a shared database. Propagated both into the
     * adapter's SQL (via {@code SchemaResolver}) and into the Flyway
     * {@code ${eventOutboxerSchema}} placeholder used by the classpath
     * migrations.
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
    inmemory,
    postgres
  }

  @Getter
  @Setter
  public static class Lock {
    /** {@code noop}, {@code postgres} or {@code redis}. */
    private LockType type = LockType.noop;

    private String keyPrefix = "outbox:lock:";
  }

  public enum LockType {
    noop,
    postgres,
    redis
  }

  @Getter
  @Setter
  public static class Cache {
    /**
     * Selects the backing store for {@code MetricsSnapshotCache}. {@code memory} (default)
     * keeps the per-JVM TTL cache from pre-SPI behaviour; {@code redis} uses the
     * {@code event-outboxer-cache-redis} module to share a single snapshot across pods;
     * {@code noop} disables caching entirely so each {@code metricsSnapshot()} call
     * recomputes from the database.
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

  @Getter
  @Setter
  public static class Maintenance {
    private Duration heartbeatInterval = Duration.ofSeconds(5);
    private Duration deadThreshold = Duration.ofSeconds(30);
    private Duration orphanRecoveryInterval = Duration.ofSeconds(30);
    private Duration watchdogInterval = Duration.ofSeconds(10);
    private int reclaimBatchSize = 50;
    private Duration shutdownTimeout = Duration.ofSeconds(30);
  }

  @Getter
  @Setter
  public static class Dispatcher {
    /** {@code SKIP}, {@code DISABLE} or {@code FAIL} for events with no registered handler. */
    private UnknownPolicy unknownHandlerPolicy = UnknownPolicy.SKIP;

    private Duration unknownHandlerRetryDelay = Duration.ofMinutes(1);
    private Duration lockBusyRetryDelay = Duration.ofSeconds(1);
    private Duration dispatchRejectedRetryDelay = Duration.ofSeconds(1);
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
   * which is what makes the thin merge in {@code OutboxEngineAutoConfiguration} possible —
   * a populated default would be indistinguishable from an explicit user value. Library
   * defaults live in {@code EventTypeConfig.defaults()} (core).
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
     * Prefix applied to every Micrometer metric published by the outbox.
     * Default: {@code event_outboxer} — a specific name chosen to avoid
     * clashing with other libraries. Override when multiple outbox
     * instances share a registry or when an organisation requires a
     * different namespace.
     */
    private String prefix = "event_outboxer";
  }

  @Getter
  @Setter
  public static class Health {
    /**
     * Actuator health groups into which the {@code outbox} indicator is
     * merged. Typical values: {@code readiness}, {@code liveness}. Default
     * is empty — no influence on any probe; the indicator lives only at
     * {@code /actuator/health/outbox}.
     *
     * <p>When set, an {@code EnvironmentPostProcessor} appends
     * {@code outbox} to {@code management.endpoint.health.group.<name>.include}
     * for each listed group, preserving the user's existing includes and
     * the default {@code <name>State} contributor so Spring Boot's
     * liveness / readiness semantics keep working.
     */
    private List<String> probeGroups = new ArrayList<>();
  }
}
