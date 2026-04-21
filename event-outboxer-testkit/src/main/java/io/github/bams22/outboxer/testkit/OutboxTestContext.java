/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.testkit;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.FailureHandler;
import io.github.bams22.outboxer.api.handle.builtin.FailureHandlers;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.publish.OutboxEventPublisher;
import io.github.bams22.outboxer.core.config.EventTypeConfig;
import io.github.bams22.outboxer.core.config.EventTypeConfigProvider;
import io.github.bams22.outboxer.core.config.MaintenanceConfig;
import io.github.bams22.outboxer.core.dispatch.DispatcherConfig;
import io.github.bams22.outboxer.core.dispatch.EventHandlerResolver;
import io.github.bams22.outboxer.core.dispatch.FailureHandlerResolver;
import io.github.bams22.outboxer.core.dispatch.HandlerDispatcher;
import io.github.bams22.outboxer.core.dispatch.InFlightRegistry;
import io.github.bams22.outboxer.core.listener.OutboxListenerRegistry;
import io.github.bams22.outboxer.core.maintenance.HeartbeatTask;
import io.github.bams22.outboxer.core.maintenance.OrphanRecoveryTask;
import io.github.bams22.outboxer.core.maintenance.WatchdogTask;
import io.github.bams22.outboxer.core.publish.DefaultOutboxEventPublisher;
import io.github.bams22.outboxer.core.publish.NoTransactionPolicy;
import io.github.bams22.outboxer.core.publish.TransactionContext;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.domain.WorkerInfo;
import io.github.bams22.outboxer.serializer.jackson.JacksonEventSerializer;
import io.github.bams22.outboxer.serializer.jackson.JacksonObjectMapperFactory;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.spi.EventSerializer;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.WorkerRegistry;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEntityLocker;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryWorkerRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One-stop test fixture that wires up the in-memory adapter, a {@link SettableClock}, a {@link
 * RecordingOutboxListener}, and a {@link ManualEngine}. Create one per test with {@link
 * #builder()}, then drive it via {@link #publisher()} and {@link #manualEngine()}.
 *
 * <p>Sensible defaults: Jackson serializer with {@link JacksonObjectMapperFactory#defaults()},
 * platform clock at system now, no entity locking, single worker id {@code "test-worker"}. Every
 * knob is overrideable via the builder.
 */
public final class OutboxTestContext {

  private final EventStore eventStore;
  private final WorkerRegistry workerRegistry;
  private final EntityLocker entityLocker;
  private final EventSerializer serializer;
  private final SettableClock clock;
  private final OutboxListenerRegistry listenerRegistry;
  private final RecordingOutboxListener recordingListener;
  private final OutboxEventPublisher publisher;
  private final ManualEngine manualEngine;
  private final WorkerInfo workerInfo;

  private OutboxTestContext(
      EventStore eventStore,
      WorkerRegistry workerRegistry,
      EntityLocker entityLocker,
      EventSerializer serializer,
      SettableClock clock,
      OutboxListenerRegistry listenerRegistry,
      RecordingOutboxListener recordingListener,
      OutboxEventPublisher publisher,
      ManualEngine manualEngine,
      WorkerInfo workerInfo) {
    this.eventStore = eventStore;
    this.workerRegistry = workerRegistry;
    this.entityLocker = entityLocker;
    this.serializer = serializer;
    this.clock = clock;
    this.listenerRegistry = listenerRegistry;
    this.recordingListener = recordingListener;
    this.publisher = publisher;
    this.manualEngine = manualEngine;
    this.workerInfo = workerInfo;
  }

  public static Builder builder() {
    return new Builder();
  }

  public EventStore eventStore() {
    return eventStore;
  }

  public WorkerRegistry workerRegistry() {
    return workerRegistry;
  }

  public EntityLocker entityLocker() {
    return entityLocker;
  }

  public EventSerializer eventSerializer() {
    return serializer;
  }

  public SettableClock clock() {
    return clock;
  }

  public OutboxListenerRegistry listenerRegistry() {
    return listenerRegistry;
  }

  public RecordingOutboxListener recording() {
    return recordingListener;
  }

  public OutboxEventPublisher publisher() {
    return publisher;
  }

  public ManualEngine manualEngine() {
    return manualEngine;
  }

  public WorkerInfo workerInfo() {
    return workerInfo;
  }

  /** Register the worker in the in-memory registry (tests that need orphan recovery call this). */
  public OutboxTestContext registerWorker() {
    workerRegistry.register(workerInfo);
    return this;
  }

  /** Deregister the worker. */
  public OutboxTestContext deregisterWorker() {
    workerRegistry.deregister(workerInfo.id());
    return this;
  }

  // =============================================================================================
  // builder
  // =============================================================================================

  /** Fluent builder for {@link OutboxTestContext}. Returns immutable contexts. */
  public static final class Builder {

    private @Nullable EventStore eventStore;
    private @Nullable WorkerRegistry workerRegistry;
    private @Nullable EntityLocker entityLocker;
    private @Nullable EventSerializer serializer;
    private @Nullable SettableClock clock;
    private @Nullable TransactionContext txContext;
    private @Nullable ObjectMapper objectMapper;
    private NoTransactionPolicy noTransactionPolicy = NoTransactionPolicy.IGNORE;
    private final List<EventHandler<?>> handlers = new ArrayList<>();
    private @Nullable FailureHandler<?> defaultFailureHandler;
    private final Map<String, FailureHandler<?>> perTypeFailureHandlers = new HashMap<>();
    private @Nullable EventTypeConfig defaultEventTypeConfig;
    private final Map<String, EventTypeConfig> perTypeOverrides = new HashMap<>();
    private @Nullable MaintenanceConfig maintenanceConfig;
    private @Nullable DispatcherConfig dispatcherConfig;
    private final List<OutboxListener> extraListeners = new ArrayList<>();
    private @Nullable WorkerId workerId;
    private String host = "test-host";
    private int defaultBatchSize = 50;

    private Builder() {}

    public Builder eventStore(EventStore store) {
      this.eventStore = Objects.requireNonNull(store);
      return this;
    }

    public Builder workerRegistry(WorkerRegistry registry) {
      this.workerRegistry = Objects.requireNonNull(registry);
      return this;
    }

    public Builder entityLocker(EntityLocker locker) {
      this.entityLocker = Objects.requireNonNull(locker);
      return this;
    }

    public Builder eventSerializer(EventSerializer s) {
      this.serializer = Objects.requireNonNull(s);
      return this;
    }

    public Builder objectMapper(ObjectMapper mapper) {
      this.objectMapper = Objects.requireNonNull(mapper);
      return this;
    }

    public Builder clock(SettableClock clock) {
      this.clock = Objects.requireNonNull(clock);
      return this;
    }

    public Builder transactionContext(TransactionContext ctx) {
      this.txContext = Objects.requireNonNull(ctx);
      return this;
    }

    public Builder noTransactionPolicy(NoTransactionPolicy policy) {
      this.noTransactionPolicy = Objects.requireNonNull(policy);
      return this;
    }

    public Builder handler(EventHandler<?> handler) {
      handlers.add(Objects.requireNonNull(handler));
      return this;
    }

    public Builder defaultFailureHandler(FailureHandler<?> fh) {
      this.defaultFailureHandler = Objects.requireNonNull(fh);
      return this;
    }

    public Builder failureHandlerFor(String eventType, FailureHandler<?> fh) {
      perTypeFailureHandlers.put(
          Objects.requireNonNull(eventType, "eventType must not be null"),
          Objects.requireNonNull(fh, "failureHandler must not be null"));
      return this;
    }

    public Builder defaultEventTypeConfig(EventTypeConfig config) {
      this.defaultEventTypeConfig = Objects.requireNonNull(config);
      return this;
    }

    public Builder eventTypeConfig(String eventType, EventTypeConfig config) {
      perTypeOverrides.put(
          Objects.requireNonNull(eventType), Objects.requireNonNull(config));
      return this;
    }

    public Builder maintenance(MaintenanceConfig cfg) {
      this.maintenanceConfig = Objects.requireNonNull(cfg);
      return this;
    }

    public Builder dispatcher(DispatcherConfig cfg) {
      this.dispatcherConfig = Objects.requireNonNull(cfg);
      return this;
    }

    public Builder listener(OutboxListener listener) {
      extraListeners.add(Objects.requireNonNull(listener));
      return this;
    }

    public Builder workerId(WorkerId id) {
      this.workerId = Objects.requireNonNull(id);
      return this;
    }

    public Builder host(String host) {
      this.host = Objects.requireNonNull(host);
      return this;
    }

    public Builder defaultBatchSize(int size) {
      if (size <= 0) {
        throw new IllegalArgumentException("defaultBatchSize must be positive, got " + size);
      }
      this.defaultBatchSize = size;
      return this;
    }

    public OutboxTestContext build() {
      SettableClock resolvedClock =
          clock != null ? clock : new SettableClock(Instant.now());
      EventStore resolvedStore =
          eventStore != null ? eventStore : new InMemoryEventStore(resolvedClock);
      WorkerRegistry resolvedRegistry =
          workerRegistry != null ? workerRegistry : new InMemoryWorkerRegistry(resolvedClock);
      EntityLocker resolvedLocker =
          entityLocker != null ? entityLocker : new InMemoryEntityLocker(resolvedClock);
      ObjectMapper resolvedMapper =
          objectMapper != null ? objectMapper : JacksonObjectMapperFactory.defaults();
      EventSerializer resolvedSerializer =
          serializer != null ? serializer : new JacksonEventSerializer(resolvedMapper);
      TransactionContext resolvedTxCtx =
          txContext != null ? txContext : TransactionContext.alwaysActive();
      WorkerId resolvedWorkerId = workerId != null ? workerId : new WorkerId("test-worker");
      EventTypeConfig resolvedDefaultTypeCfg =
          defaultEventTypeConfig != null ? defaultEventTypeConfig : testDefaults();
      MaintenanceConfig resolvedMaintenance =
          maintenanceConfig != null ? maintenanceConfig : testMaintenance();
      DispatcherConfig resolvedDispatcher =
          dispatcherConfig != null ? dispatcherConfig : DispatcherConfig.defaults();
      FailureHandler<?> resolvedDefaultFailure =
          defaultFailureHandler != null ? defaultFailureHandler : FailureHandlers.defaults();

      RecordingOutboxListener recording = new RecordingOutboxListener();
      OutboxListenerRegistry listeners = new OutboxListenerRegistry();
      listeners.add(recording);
      for (OutboxListener l : extraListeners) {
        listeners.add(l);
      }

      EventTypeConfigProvider typeCfg =
          new EventTypeConfigProvider(resolvedDefaultTypeCfg, perTypeOverrides);
      EventHandlerResolver handlerResolver = new EventHandlerResolver(handlers);
      FailureHandlerResolver failureResolver =
          new FailureHandlerResolver(perTypeFailureHandlers, resolvedDefaultFailure);
      InFlightRegistry inFlight = new InFlightRegistry();

      HandlerDispatcher dispatcher =
          new HandlerDispatcher(
              resolvedStore,
              resolvedLocker,
              resolvedSerializer,
              handlerResolver,
              failureResolver,
              inFlight,
              listeners,
              resolvedClock,
              resolvedWorkerId,
              typeCfg,
              resolvedDispatcher);

      HeartbeatTask heartbeat =
          new HeartbeatTask(resolvedRegistry, resolvedWorkerId, resolvedClock, listeners);
      OrphanRecoveryTask orphan =
          new OrphanRecoveryTask(
              resolvedRegistry, resolvedStore, resolvedClock, resolvedMaintenance, listeners);
      WatchdogTask watchdog =
          new WatchdogTask(inFlight, resolvedStore, resolvedClock, typeCfg, listeners);

      List<String> types = new ArrayList<>(handlerResolver.registeredTypes());

      ManualEngine engine =
          new ManualEngine(
              resolvedStore,
              resolvedWorkerId,
              dispatcher,
              heartbeat,
              orphan,
              watchdog,
              types,
              defaultBatchSize);

      OutboxEventPublisher publisher =
          new DefaultOutboxEventPublisher(
              resolvedStore,
              resolvedSerializer,
              resolvedClock,
              resolvedTxCtx,
              noTransactionPolicy,
              listeners);

      WorkerInfo workerInfo =
          WorkerInfo.builder()
              .id(resolvedWorkerId)
              .host(host)
              .pid((int) ProcessHandle.current().pid())
              .startedAt(resolvedClock.now())
              .metadata(Map.of())
              .build();

      return new OutboxTestContext(
          resolvedStore,
          resolvedRegistry,
          resolvedLocker,
          resolvedSerializer,
          resolvedClock,
          listeners,
          recording,
          publisher,
          engine,
          workerInfo);
    }

    private static EventTypeConfig testDefaults() {
      return EventTypeConfig.builder()
          .pollMinInterval(Duration.ofMillis(10))
          .pollMaxInterval(Duration.ofMillis(50))
          .pollMultiplier(1.5)
          .claimBatchSize(50)
          .handlerPoolSize(1)
          .handlerQueueCapacity(100)
          .handlerMaxRuntime(Duration.ofMinutes(1))
          .lockTtl(Duration.ofMinutes(1))
          .build();
    }

    private static MaintenanceConfig testMaintenance() {
      return MaintenanceConfig.builder()
          .heartbeatInterval(Duration.ofSeconds(1))
          .deadThreshold(Duration.ofSeconds(3))
          .orphanRecoveryInterval(Duration.ofSeconds(1))
          .watchdogInterval(Duration.ofMillis(500))
          .reclaimBatchSize(50)
          .shutdownTimeout(Duration.ofSeconds(1))
          .build();
    }
  }
}
