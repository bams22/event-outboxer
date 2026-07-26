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

import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.FailureHandler;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.publish.OutboxEventPublisher;
import io.github.bams22.outboxer.core.config.EventTypeConfig;
import io.github.bams22.outboxer.core.config.MaintenanceConfig;
import io.github.bams22.outboxer.core.dispatch.DispatcherConfig;
import io.github.bams22.outboxer.core.engine.OutboxEngine;
import io.github.bams22.outboxer.core.engine.OutboxEngineBuilder;
import io.github.bams22.outboxer.core.publish.NoTransactionPolicy;
import io.github.bams22.outboxer.core.publish.TransactionContext;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.spi.EventSerializer;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.WorkerRegistry;
import io.github.bams22.outboxer.spring.executor.HandlerExecutorFactory;
import io.github.bams22.outboxer.spring.lifecycle.OutboxSmartLifecycle;
import io.github.bams22.outboxer.spring.lock.NoOpLockAutoConfiguration;
import io.github.bams22.outboxer.spring.lock.PostgresLockAutoConfiguration;
import io.github.bams22.outboxer.spring.lock.RedisLockAutoConfiguration;
import io.github.bams22.outboxer.spring.publisher.SpringTransactionContext;
import io.github.bams22.outboxer.spring.serializer.JacksonSerializerAutoConfiguration;
import io.github.bams22.outboxer.spring.storage.InMemoryStorageAutoConfiguration;
import io.github.bams22.outboxer.spring.storage.PostgresStorageAutoConfiguration;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Central wiring of the outbox engine. Consumes the lower-level auto-configurations
 * ({@code InMemoryStorageAutoConfiguration}, {@code PostgresStorageAutoConfiguration}, lock and
 * serializer variants) and composes them into a single {@link OutboxEngine} managed by
 * {@link OutboxSmartLifecycle}.
 */
@AutoConfiguration(
    after = {
      InMemoryStorageAutoConfiguration.class,
      PostgresStorageAutoConfiguration.class,
      NoOpLockAutoConfiguration.class,
      PostgresLockAutoConfiguration.class,
      RedisLockAutoConfiguration.class,
      JacksonSerializerAutoConfiguration.class
    })
@EnableConfigurationProperties(OutboxProperties.class)
@ConditionalOnProperty(prefix = "event-outboxer", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OutboxEngineAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public Clock outboxClock() {
    return Clock.system();
  }

  @Bean
  @ConditionalOnMissingBean
  public TransactionContext outboxTransactionContext() {
    return new SpringTransactionContext();
  }

  /**
   * Shared wake hub: the publisher bean below and the engine's pollers are constructed
   * independently, so the after-commit poller wake-up needs one hub visible to both.
   */
  @Bean
  @ConditionalOnMissingBean
  public io.github.bams22.outboxer.core.polling.PollerWakeHub outboxPollerWakeHub() {
    return new io.github.bams22.outboxer.core.polling.PollerWakeHub();
  }

  @Bean
  @ConditionalOnMissingBean
  public WorkerId outboxWorkerId(OutboxProperties properties) {
    String explicit = properties.getWorker().getId();
    return explicit != null && !explicit.isBlank()
        ? new WorkerId(explicit)
        : WorkerId.generateDefault();
  }

  @Bean
  @ConditionalOnMissingBean(OutboxEventPublisher.class)
  public OutboxEventPublisher outboxEventPublisher(
      EventStore store,
      EventSerializer serializer,
      Clock clock,
      TransactionContext txContext,
      OutboxProperties properties,
      io.github.bams22.outboxer.core.polling.PollerWakeHub wakeHub,
      List<OutboxListener> listeners) {
    NoTransactionPolicy policy = mapNoTxPolicy(properties.getPublisher().getNoTransactionPolicy());
    // Publisher fires listener callbacks directly; the engine's ListenerRegistry will subsume
    // these when the engine starts — for now, aggregate all application-registered listeners.
    OutboxListener fanout = new FanOutListener(listeners);
    return new io.github.bams22.outboxer.core.publish.DefaultOutboxEventPublisher(
        store, serializer, clock, txContext, policy, fanout, wakeHub);
  }

  @Bean
  @ConditionalOnMissingBean
  public OutboxEngine outboxEngine(
      EventStore store,
      WorkerRegistry registry,
      EntityLocker locker,
      EventSerializer serializer,
      Clock clock,
      TransactionContext txContext,
      WorkerId workerId,
      OutboxProperties properties,
      io.github.bams22.outboxer.core.polling.PollerWakeHub wakeHub,
      ObjectProvider<EventHandler<?>> handlerProvider,
      @Qualifier("outboxDefaultFailureHandler") ObjectProvider<FailureHandler<?>>
              defaultFailureHandlerProvider,
      @Qualifier("outboxPerTypeFailureHandlers") ObjectProvider<Map<String, FailureHandler<?>>>
              perTypeFailureHandlersProvider,
      ObjectProvider<io.github.bams22.outboxer.core.polling.PollStrategy> pollStrategyProvider,
      ObjectProvider<org.springframework.core.task.TaskDecorator> taskDecoratorProvider,
      List<OutboxListener> listeners) {

    OutboxEngineBuilder builder =
        new OutboxEngineBuilder()
            .eventStore(store)
            .workerRegistry(registry)
            .eventSerializer(serializer)
            .entityLocker(locker)
            .clock(clock)
            .transactionContext(txContext)
            .noTransactionPolicy(mapNoTxPolicy(properties.getPublisher().getNoTransactionPolicy()))
            .workerIdSupplier(() -> workerId)
            .wakeHub(wakeHub)
            .maintenance(mapMaintenance(properties.getMaintenance()))
            .dispatcher(mapDispatcher(properties.getDispatcher()));

    // Thin merge (CONFIGURATION.md §Per-type override): user defaults overlay the library
    // defaults, and each per-type override overlays the resolved defaults field by field —
    // an override that sets only handler-pool-size keeps every other default intact.
    EventTypeConfig resolvedDefaults =
        mergeEventType(properties.getEventTypes().getDefaults(), EventTypeConfig.defaults());
    builder.defaultEventTypeConfig(resolvedDefaults);
    for (Map.Entry<String, OutboxProperties.EventType> e :
        properties.getEventTypes().getOverrides().entrySet()) {
      builder.eventTypeConfig(e.getKey(), mergeEventType(e.getValue(), resolvedDefaults));
    }

    String host = properties.getWorker().getHost();
    if (host != null && !host.isBlank()) {
      builder.host(host);
    }
    for (Map.Entry<String, String> m : properties.getWorker().getMetadata().entrySet()) {
      builder.workerMetadata(m.getKey(), m.getValue());
    }

    handlerProvider.forEach(builder::handler);
    defaultFailureHandlerProvider.ifAvailable(builder::defaultFailureHandler);
    perTypeFailureHandlersProvider.ifAvailable(
        map -> map.forEach(builder::failureHandlerFor));
    pollStrategyProvider.ifAvailable(builder::pollStrategy);
    for (OutboxListener l : listeners) {
      builder.listener(l);
    }
    // Starter handles logging via application SLF4J config; avoid double-logging.
    builder.includeLoggingListener(false);

    // Wire handler executor factory per outbox.handler-executor.type. A user-provided
    // @Bean TaskDecorator wins; otherwise fall back to Spring's ContextPropagatingTaskDecorator
    // (which already propagates MDC / Observation / security context).
    org.springframework.core.task.TaskDecorator decorator =
        taskDecoratorProvider.getIfAvailable(
            org.springframework.core.task.support.ContextPropagatingTaskDecorator::new);
    switch (properties.getHandlerExecutor().getType()) {
      case virtual -> builder.handlerExecutorFactory(HandlerExecutorFactory.virtual(decorator));
      case platform -> builder.handlerExecutorFactory(HandlerExecutorFactory.platform(decorator));
    }

    return builder.build();
  }

  @Bean
  @ConditionalOnMissingBean
  public OutboxSmartLifecycle outboxSmartLifecycle(OutboxEngine engine) {
    return new OutboxSmartLifecycle(engine);
  }

  // ---------------------------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------------------------

  private static NoTransactionPolicy mapNoTxPolicy(OutboxProperties.NoTxPolicy p) {
    return switch (p) {
      case FAIL -> NoTransactionPolicy.FAIL;
      case IGNORE -> NoTransactionPolicy.IGNORE;
    };
  }

  private static MaintenanceConfig mapMaintenance(OutboxProperties.Maintenance m) {
    return MaintenanceConfig.builder()
        .heartbeatInterval(m.getHeartbeatInterval())
        .deadThreshold(m.getDeadThreshold())
        .orphanRecoveryInterval(m.getOrphanRecoveryInterval())
        .watchdogInterval(m.getWatchdogInterval())
        .reclaimBatchSize(m.getReclaimBatchSize())
        .shutdownTimeout(m.getShutdownTimeout())
        .build();
  }

  private static DispatcherConfig mapDispatcher(OutboxProperties.Dispatcher d) {
    return DispatcherConfig.builder()
        .unknownHandlerPolicy(
            switch (d.getUnknownHandlerPolicy()) {
              case SKIP -> io.github.bams22.outboxer.core.config.UnknownHandlerPolicy.SKIP;
              case DISABLE -> io.github.bams22.outboxer.core.config.UnknownHandlerPolicy.DISABLE;
              case FAIL -> io.github.bams22.outboxer.core.config.UnknownHandlerPolicy.FAIL;
            })
        .unknownHandlerRetryDelay(d.getUnknownHandlerRetryDelay())
        .lockBusyRetryDelay(d.getLockBusyRetryDelay())
        .dispatchRejectedRetryDelay(d.getDispatchRejectedRetryDelay())
        .build();
  }

  /**
   * Field-by-field overlay of a (possibly sparse) properties object onto a fully resolved base
   * config — the "thin merge" documented in CONFIGURATION.md. Package-private for tests.
   */
  static EventTypeConfig mergeEventType(OutboxProperties.EventType e, EventTypeConfig base) {
    return EventTypeConfig.builder()
        .pollMinInterval(
            e.getPollMinInterval() != null ? e.getPollMinInterval() : base.pollMinInterval())
        .pollMaxInterval(
            e.getPollMaxInterval() != null ? e.getPollMaxInterval() : base.pollMaxInterval())
        .pollMultiplier(
            e.getPollMultiplier() != null ? e.getPollMultiplier() : base.pollMultiplier())
        .claimBatchSize(
            e.getClaimBatchSize() != null ? e.getClaimBatchSize() : base.claimBatchSize())
        .handlerPoolSize(
            e.getHandlerPoolSize() != null ? e.getHandlerPoolSize() : base.handlerPoolSize())
        .handlerQueueCapacity(
            e.getHandlerQueueCapacity() != null
                ? e.getHandlerQueueCapacity()
                : base.handlerQueueCapacity())
        .handlerMaxRuntime(
            e.getHandlerMaxRuntime() != null ? e.getHandlerMaxRuntime() : base.handlerMaxRuntime())
        .lockTtl(e.getLockTtl() != null ? e.getLockTtl() : base.lockTtl())
        .build();
  }

  /** Simple fan-out to avoid constructing the core's ListenerRegistry here. */
  private static final class FanOutListener implements OutboxListener {
    private final List<OutboxListener> delegates;

    FanOutListener(List<OutboxListener> delegates) {
      this.delegates = List.copyOf(delegates);
    }

    @Override
    public void onEventPublished(io.github.bams22.outboxer.api.observer.EventPublishedInfo info) {
      for (OutboxListener l : delegates) {
        try {
          l.onEventPublished(info);
        } catch (RuntimeException ignored) {
          // isolation: one broken listener must not poison a publish
        }
      }
    }
  }
}
