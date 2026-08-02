/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.engine;

import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.FailureHandler;
import io.github.bams22.outboxer.api.handle.builtin.FailureHandlers;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.publish.OutboxEventPublisher;
import io.github.bams22.outboxer.core.concurrent.NamedThreadFactory;
import io.github.bams22.outboxer.core.config.EventTypeConfig;
import io.github.bams22.outboxer.core.config.EventTypeConfigProvider;
import io.github.bams22.outboxer.core.config.MaintenanceConfig;
import io.github.bams22.outboxer.core.config.RetentionConfig;
import io.github.bams22.outboxer.core.dispatch.DispatcherConfig;
import io.github.bams22.outboxer.core.dispatch.EventHandlerResolver;
import io.github.bams22.outboxer.core.dispatch.FailureHandlerResolver;
import io.github.bams22.outboxer.core.dispatch.GroupCommitEventStore;
import io.github.bams22.outboxer.core.dispatch.HandlerDispatcher;
import io.github.bams22.outboxer.core.dispatch.InFlightRegistry;
import io.github.bams22.outboxer.core.listener.LoggingOutboxListener;
import io.github.bams22.outboxer.core.listener.OutboxListenerRegistry;
import io.github.bams22.outboxer.core.maintenance.EngineHealthCheckTask;
import io.github.bams22.outboxer.core.maintenance.HeartbeatTask;
import io.github.bams22.outboxer.core.maintenance.MaintenanceScheduler;
import io.github.bams22.outboxer.core.maintenance.OrphanRecoveryTask;
import io.github.bams22.outboxer.core.maintenance.RetentionTask;
import io.github.bams22.outboxer.core.maintenance.StaleClaimSweeperTask;
import io.github.bams22.outboxer.core.maintenance.WatchdogTask;
import io.github.bams22.outboxer.core.polling.LockAndFetchStrategy;
import io.github.bams22.outboxer.core.polling.PollStrategy;
import io.github.bams22.outboxer.core.polling.Poller;
import io.github.bams22.outboxer.core.polling.PollerWakeHub;
import io.github.bams22.outboxer.core.publish.DefaultOutboxEventPublisher;
import io.github.bams22.outboxer.core.publish.NoTransactionPolicy;
import io.github.bams22.outboxer.core.publish.TransactionContext;
import io.github.bams22.outboxer.core.tracing.SafeOutboxTracer;
import io.github.bams22.outboxer.core.workerid.WorkerIdFactory;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.domain.WorkerInfo;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.spi.EventSerializer;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.OutboxAdmin;
import io.github.bams22.outboxer.spi.OutboxTracer;
import io.github.bams22.outboxer.spi.WorkerRegistry;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;

/**
 * Plain-Java builder for {@link OutboxEngine}. The Spring Boot starter (P9) wires Spring-managed
 * collaborators directly into the engine constructor instead of going through this builder.
 *
 * <p>Required collaborators: {@link EventStore}, {@link WorkerRegistry}, {@link EventSerializer},
 * and at least one {@link EventHandler}. Every other knob has a default.
 */
public final class OutboxEngineBuilder {

  private @Nullable EventStore store;
  private @Nullable WorkerRegistry registry;
  private @Nullable EventSerializer serializer;
  private EntityLocker locker = EntityLocker.NOOP;
  private Clock clock = Clock.system();
  private TransactionContext txContext = TransactionContext.alwaysActive();
  private NoTransactionPolicy noTxPolicy = NoTransactionPolicy.IGNORE;
  private final List<EventHandler<?>> handlers = new ArrayList<>();
  private @Nullable FailureHandler<?> defaultFailureHandler;
  private final Map<String, FailureHandler<?>> perTypeFailureHandlers = new HashMap<>();
  private EventTypeConfig defaultEventTypeConfig = EventTypeConfig.defaults();
  private final Map<String, EventTypeConfig> perTypeOverrides = new HashMap<>();
  private MaintenanceConfig maintenanceConfig = MaintenanceConfig.defaults();
  private DispatcherConfig dispatcherConfig = DispatcherConfig.defaults();
  private final List<OutboxListener> listeners = new ArrayList<>();
  private boolean includeLoggingListener = true;
  private OutboxTracer tracer = OutboxTracer.NOOP;
  private Supplier<WorkerId> workerIdSupplier = WorkerIdFactory.defaultGenerator();
  private @Nullable String host;
  private final Map<String, String> workerMetadata = new LinkedHashMap<>();
  private @Nullable Function<EventTypeConfig, ExecutorService> handlerExecutorFactory;
  private @Nullable PollStrategy pollStrategy;
  private @Nullable PollerWakeHub wakeHub;
  private @Nullable OutboxAdmin admin;
  private RetentionConfig retentionConfig = RetentionConfig.disabled();

  // ---------------------------------------------------------------------------------------------
  // required collaborators
  // ---------------------------------------------------------------------------------------------

  public OutboxEngineBuilder eventStore(EventStore store) {
    this.store = Objects.requireNonNull(store);
    return this;
  }

  public OutboxEngineBuilder workerRegistry(WorkerRegistry registry) {
    this.registry = Objects.requireNonNull(registry);
    return this;
  }

  public OutboxEngineBuilder eventSerializer(EventSerializer serializer) {
    this.serializer = Objects.requireNonNull(serializer);
    return this;
  }

  public OutboxEngineBuilder entityLocker(EntityLocker locker) {
    this.locker = Objects.requireNonNull(locker);
    return this;
  }

  public OutboxEngineBuilder clock(Clock clock) {
    this.clock = Objects.requireNonNull(clock);
    return this;
  }

  public OutboxEngineBuilder transactionContext(TransactionContext txContext) {
    this.txContext = Objects.requireNonNull(txContext);
    return this;
  }

  public OutboxEngineBuilder noTransactionPolicy(NoTransactionPolicy policy) {
    this.noTxPolicy = Objects.requireNonNull(policy);
    return this;
  }

  public OutboxEngineBuilder handler(EventHandler<?> handler) {
    handlers.add(Objects.requireNonNull(handler));
    return this;
  }

  public OutboxEngineBuilder handlers(List<? extends EventHandler<?>> list) {
    Objects.requireNonNull(list, "list must not be null");
    for (EventHandler<?> h : list) {
      handler(h);
    }
    return this;
  }

  public OutboxEngineBuilder defaultFailureHandler(FailureHandler<?> fh) {
    this.defaultFailureHandler = Objects.requireNonNull(fh);
    return this;
  }

  public OutboxEngineBuilder failureHandlerFor(String eventType, FailureHandler<?> fh) {
    Objects.requireNonNull(eventType, "eventType must not be null");
    Objects.requireNonNull(fh, "fh must not be null");
    perTypeFailureHandlers.put(eventType, fh);
    return this;
  }

  public OutboxEngineBuilder defaultEventTypeConfig(EventTypeConfig cfg) {
    this.defaultEventTypeConfig = Objects.requireNonNull(cfg);
    return this;
  }

  public OutboxEngineBuilder eventTypeConfig(String eventType, EventTypeConfig cfg) {
    Objects.requireNonNull(eventType, "eventType must not be null");
    Objects.requireNonNull(cfg, "cfg must not be null");
    perTypeOverrides.put(eventType, cfg);
    return this;
  }

  public OutboxEngineBuilder maintenance(MaintenanceConfig cfg) {
    this.maintenanceConfig = Objects.requireNonNull(cfg);
    return this;
  }

  public OutboxEngineBuilder dispatcher(DispatcherConfig cfg) {
    this.dispatcherConfig = Objects.requireNonNull(cfg);
    return this;
  }

  public OutboxEngineBuilder listener(OutboxListener listener) {
    listeners.add(Objects.requireNonNull(listener));
    return this;
  }

  public OutboxEngineBuilder includeLoggingListener(boolean include) {
    this.includeLoggingListener = include;
    return this;
  }

  /**
   * Tracing port (ADR-0023): PRODUCER span around every event insert, CONSUMER span around every
   * handler invocation. Default: {@link OutboxTracer#NOOP}. The builder wraps the tracer
   * defensively, so implementation failures degrade to no-op instead of breaking dispatch.
   */
  public OutboxEngineBuilder tracer(OutboxTracer tracer) {
    this.tracer = Objects.requireNonNull(tracer);
    return this;
  }

  public OutboxEngineBuilder workerIdSupplier(Supplier<WorkerId> supplier) {
    this.workerIdSupplier = Objects.requireNonNull(supplier);
    return this;
  }

  public OutboxEngineBuilder host(String host) {
    this.host = Objects.requireNonNull(host);
    return this;
  }

  public OutboxEngineBuilder workerMetadata(String key, String value) {
    Objects.requireNonNull(key, "key must not be null");
    Objects.requireNonNull(value, "value must not be null");
    workerMetadata.put(key, value);
    return this;
  }

  public OutboxEngineBuilder handlerExecutorFactory(
      Function<EventTypeConfig, ExecutorService> factory) {
    this.handlerExecutorFactory = Objects.requireNonNull(factory);
    return this;
  }

  public OutboxEngineBuilder pollStrategy(PollStrategy strategy) {
    this.pollStrategy = Objects.requireNonNull(strategy);
    return this;
  }

  /**
   * Externally owned {@link PollerWakeHub} to register this engine's pollers in. Used by the Spring
   * Boot starter, where the {@code OutboxEventPublisher} bean is built independently of the engine
   * and both must share one hub. When unset, the builder creates a private hub for the
   * engine-internal publisher.
   */
  public OutboxEngineBuilder wakeHub(PollerWakeHub hub) {
    this.wakeHub = Objects.requireNonNull(hub);
    return this;
  }

  /**
   * Admin port used by the optional retention task. The engine itself never calls admin operations;
   * the port is only consumed when {@link #retention(RetentionConfig)} enables at least one
   * threshold.
   */
  public OutboxEngineBuilder admin(OutboxAdmin admin) {
    this.admin = Objects.requireNonNull(admin);
    return this;
  }

  /** Retention thresholds (ADR-0019). Default: fully disabled. */
  public OutboxEngineBuilder retention(RetentionConfig config) {
    this.retentionConfig = Objects.requireNonNull(config);
    return this;
  }

  // ---------------------------------------------------------------------------------------------
  // build
  // ---------------------------------------------------------------------------------------------

  public OutboxEngine build() {
    EventStore eventStore = require(store, "eventStore");
    WorkerRegistry workerRegistry = require(registry, "workerRegistry");
    EventSerializer eventSerializer = require(serializer, "eventSerializer");
    if (handlers.isEmpty()) {
      throw new IllegalStateException("at least one EventHandler must be registered");
    }

    OutboxListenerRegistry listener = new OutboxListenerRegistry(listeners);
    if (includeLoggingListener) {
      listener.add(new LoggingOutboxListener());
    }

    FailureHandler<?> defaults =
        defaultFailureHandler != null ? defaultFailureHandler : FailureHandlers.defaults();
    FailureHandlerResolver failureResolver =
        new FailureHandlerResolver(perTypeFailureHandlers, defaults);
    EventHandlerResolver handlerResolver = new EventHandlerResolver(handlers);
    EventTypeConfigProvider typeConfig =
        new EventTypeConfigProvider(defaultEventTypeConfig, perTypeOverrides);
    InFlightRegistry inFlight = new InFlightRegistry();

    WorkerId workerId = workerIdSupplier.get();
    WorkerInfo workerInfo =
        WorkerInfo.builder()
            .id(workerId)
            .host(host != null ? host : resolveHost())
            .pid((int) ProcessHandle.current().pid())
            .startedAt(clock.now())
            .metadata(Map.copyOf(workerMetadata))
            .build();

    // Group-commit batching applies only to the dispatcher's finalize path: pollers, publisher
    // and maintenance keep the raw store (their operations are either already batched or rare).
    EventStore dispatcherStore =
        dispatcherConfig.finalizeBatching()
            ? new GroupCommitEventStore(
                eventStore, workerId, dispatcherConfig.finalizeBatchMaxSize())
            : eventStore;

    // Wrap once here so both the dispatcher and the publisher share the same shielded instance.
    OutboxTracer safeTracer = SafeOutboxTracer.wrap(tracer);

    HandlerDispatcher dispatcher =
        new HandlerDispatcher(
            dispatcherStore,
            locker,
            eventSerializer,
            handlerResolver,
            failureResolver,
            inFlight,
            listener,
            clock,
            workerId,
            typeConfig,
            dispatcherConfig,
            safeTracer);

    PollStrategy strategy =
        pollStrategy != null ? pollStrategy : new LockAndFetchStrategy(eventStore);
    Function<EventTypeConfig, ExecutorService> executorFactory =
        handlerExecutorFactory != null ? handlerExecutorFactory : defaultExecutorFactory();

    Map<String, EventTypeConfig> executorConfigs = new LinkedHashMap<>();
    for (EventHandler<?> h : handlers) {
      executorConfigs.put(h.eventType(), typeConfig.forType(h.eventType()));
    }
    HandlerExecutorManager executors = new HandlerExecutorManager(executorFactory, executorConfigs);

    PollerWakeHub hub = wakeHub != null ? wakeHub : new PollerWakeHub();
    List<Poller> pollers = new ArrayList<>(handlers.size());
    for (EventHandler<?> h : handlers) {
      String type = h.eventType();
      EventTypeConfig cfg =
          Objects.requireNonNull(executorConfigs.get(type), "no executor config for type " + type);
      io.github.bams22.outboxer.core.polling.HandlerExecutorGate gate = executors.executorFor(type);
      Poller poller = new Poller(type, workerId, strategy, dispatcher, gate, listener, cfg);
      // Couple the two ends of the pipeline: a saturated executor freeing a slot wakes the
      // poller immediately instead of costing the remainder of the poll interval.
      gate.onCapacityAvailable(poller::wake);
      hub.register(poller);
      pollers.add(poller);
    }

    OutboxEventPublisher publisher =
        new DefaultOutboxEventPublisher(
            eventStore, eventSerializer, clock, txContext, noTxPolicy, listener, hub, safeTracer);

    HeartbeatTask heartbeat = new HeartbeatTask(workerRegistry, workerInfo, clock, listener);
    OrphanRecoveryTask orphanTask =
        new OrphanRecoveryTask(workerRegistry, eventStore, clock, maintenanceConfig, listener);
    WatchdogTask watchdog = new WatchdogTask(inFlight, eventStore, clock, typeConfig, listener);

    // The engine-health-check needs to report crashes to the engine, but the engine isn't
    // constructed yet. Resolve with an AtomicReference the lambda dereferences at run time.
    java.util.concurrent.atomic.AtomicReference<OutboxEngine> engineRef =
        new java.util.concurrent.atomic.AtomicReference<>();
    EngineHealthCheckTask engineHealthCheck =
        new EngineHealthCheckTask(
            pollers,
            (reason, cause) -> {
              OutboxEngine e = engineRef.get();
              if (e != null) {
                e.markCrashed(reason, cause);
              }
            });
    RetentionTask retention =
        admin != null && retentionConfig.enabled()
            ? new RetentionTask(admin, clock, retentionConfig)
            : null;
    StaleClaimSweeperTask staleClaimSweeper =
        new StaleClaimSweeperTask(
            eventStore,
            resolveStaleClaimThreshold(maintenanceConfig, executorConfigs.values()),
            maintenanceConfig.staleClaimSweepInterval(),
            maintenanceConfig.reclaimBatchSize());
    MaintenanceScheduler maintenance =
        new MaintenanceScheduler(
            heartbeat,
            orphanTask,
            watchdog,
            engineHealthCheck,
            retention,
            staleClaimSweeper,
            maintenanceConfig);

    OutboxEngine engine =
        new OutboxEngine(
            workerRegistry,
            workerInfo,
            eventStore,
            clock,
            publisher,
            maintenance,
            pollers,
            executors,
            listener,
            maintenanceConfig.shutdownTimeout());
    engineRef.set(engine);
    return engine;
  }

  /**
   * The sweep threshold must exceed every per-type {@code handlerMaxRuntime}: the sweeper cannot
   * see any JVM's in-flight registry, so a smaller threshold would reclaim a legitimately
   * long-running handler's row on another pod. When unset, derive {@code 2 × max}; an explicit
   * value that violates the invariant fails the build. Heterogeneous fleets (a rolling deploy that
   * raises {@code handlerMaxRuntime}) should set the threshold explicitly with headroom.
   */
  static Duration resolveStaleClaimThreshold(
      MaintenanceConfig maintenance, java.util.Collection<EventTypeConfig> typeConfigs) {
    Duration maxRuntime = Duration.ZERO;
    for (EventTypeConfig cfg : typeConfigs) {
      if (cfg.handlerMaxRuntime().compareTo(maxRuntime) > 0) {
        maxRuntime = cfg.handlerMaxRuntime();
      }
    }
    Duration explicit = maintenance.staleClaimThreshold();
    if (explicit == null) {
      return maxRuntime.multipliedBy(2);
    }
    if (explicit.compareTo(maxRuntime) <= 0) {
      throw new IllegalArgumentException(
          "staleClaimThreshold ("
              + explicit
              + ") must exceed the largest per-type handlerMaxRuntime ("
              + maxRuntime
              + ") — otherwise the sweeper would reclaim rows of legitimately running handlers "
              + "on other instances");
    }
    return explicit;
  }

  // ---------------------------------------------------------------------------------------------
  // defaults
  // ---------------------------------------------------------------------------------------------

  private static Function<EventTypeConfig, ExecutorService> defaultExecutorFactory() {
    return cfg -> {
      BlockingQueue<Runnable> queue =
          cfg.handlerQueueCapacity() == 0
              ? new SynchronousQueue<>()
              : (cfg.handlerQueueCapacity() > 0
                  ? new ArrayBlockingQueue<>(cfg.handlerQueueCapacity())
                  : new LinkedBlockingQueue<>());
      return new ThreadPoolExecutor(
          cfg.handlerPoolSize(),
          cfg.handlerPoolSize(),
          0L,
          TimeUnit.MILLISECONDS,
          queue,
          new NamedThreadFactory("outbox-handler", true),
          new ThreadPoolExecutor.AbortPolicy());
    };
  }

  private static String resolveHost() {
    try {
      String h = InetAddress.getLocalHost().getHostName();
      if (h != null && !h.isBlank()) {
        return h;
      }
    } catch (UnknownHostException _) {
      // fall through
    }
    String name = ManagementFactory.getRuntimeMXBean().getName();
    int at = name.indexOf('@');
    return at >= 0 && at + 1 < name.length() ? name.substring(at + 1) : "unknown-host";
  }

  private static <T> T require(@Nullable T value, String name) {
    if (value == null) {
      throw new IllegalStateException(name + " must be configured before build()");
    }
    return value;
  }
}
