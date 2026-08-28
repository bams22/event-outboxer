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

import com.zaxxer.hikari.HikariDataSource;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.FailureHandler;
import io.github.bams22.outboxer.api.observer.EventPublishedInfo;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.publish.OutboxEventPublisher;
import io.github.bams22.outboxer.core.config.EventTypeConfig;
import io.github.bams22.outboxer.core.config.MaintenanceConfig;
import io.github.bams22.outboxer.core.config.RetentionConfig;
import io.github.bams22.outboxer.core.config.UnknownHandlerPolicy;
import io.github.bams22.outboxer.core.dispatch.DispatcherConfig;
import io.github.bams22.outboxer.core.engine.OutboxEngine;
import io.github.bams22.outboxer.core.engine.OutboxEngineBuilder;
import io.github.bams22.outboxer.core.polling.PollStrategy;
import io.github.bams22.outboxer.core.polling.PollerWakeHub;
import io.github.bams22.outboxer.core.publish.DefaultOutboxEventPublisher;
import io.github.bams22.outboxer.core.publish.NoTransactionPolicy;
import io.github.bams22.outboxer.core.publish.TransactionContext;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.spi.EventSerializer;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.OutboxAdmin;
import io.github.bams22.outboxer.spi.OutboxTracer;
import io.github.bams22.outboxer.spi.WorkerRegistry;
import io.github.bams22.outboxer.spring.executor.HandlerExecutorFactory;
import io.github.bams22.outboxer.spring.lifecycle.OutboxSmartLifecycle;
import io.github.bams22.outboxer.spring.lock.NoOpLockAutoConfiguration;
import io.github.bams22.outboxer.spring.lock.PostgresAdvisoryLockAutoConfiguration;
import io.github.bams22.outboxer.spring.lock.PostgresLeaseLockAutoConfiguration;
import io.github.bams22.outboxer.spring.lock.RedisLockAutoConfiguration;
import io.github.bams22.outboxer.spring.publisher.SpringTransactionContext;
import io.github.bams22.outboxer.spring.serializer.JacksonSerializerAutoConfiguration;
import io.github.bams22.outboxer.spring.serializer.OutboxSerializers;
import io.github.bams22.outboxer.spring.storage.PostgresStorageAutoConfiguration;
import io.github.bams22.outboxer.spring.tracing.MicrometerTracingAutoConfiguration;
import io.github.bams22.outboxer.spring.tracing.OtelTracingAutoConfiguration;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskDecorator;
import org.springframework.core.task.support.ContextPropagatingTaskDecorator;
import org.springframework.util.ClassUtils;

/**
 * Central wiring of the outbox engine. Consumes the lower-level auto-configurations ({@code
 * PostgresStorageAutoConfiguration}, lock and serializer variants) and composes them into a single
 * {@link OutboxEngine} managed by {@link OutboxSmartLifecycle}. There is no in-memory storage
 * auto-configuration on purpose (ADR-0020): a durable store is the point of the library; tests
 * import {@code OutboxInMemoryTestConfiguration} explicitly.
 */
@AutoConfiguration(
        after = {
            PostgresStorageAutoConfiguration.class,
            NoOpLockAutoConfiguration.class,
            PostgresLeaseLockAutoConfiguration.class,
            PostgresAdvisoryLockAutoConfiguration.class,
            RedisLockAutoConfiguration.class,
            JacksonSerializerAutoConfiguration.class,
            MicrometerTracingAutoConfiguration.class,
            OtelTracingAutoConfiguration.class
        })
@EnableConfigurationProperties(OutboxProperties.class)
@ConditionalOnProperty(
        prefix = "event-outboxer",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true)
public class OutboxEngineAutoConfiguration {

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(OutboxEngineAutoConfiguration.class);

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
    public PollerWakeHub outboxPollerWakeHub() {
        return new PollerWakeHub();
    }

    @Bean
    @ConditionalOnMissingBean
    public WorkerId outboxWorkerId(OutboxProperties properties) {
        String explicit = properties.getWorker().getId();
        return explicit != null && !explicit.isBlank()
                ? new WorkerId(explicit)
                : WorkerId.generateDefault();
    }

    /**
     * All registered {@code EventSerializer} beans (keyed by bean name) resolved into the write
     * serializer + read-only rest (ADR-0025). See {@link OutboxSerializers#resolve}.
     */
    @Bean
    @ConditionalOnMissingBean
    public OutboxSerializers outboxSerializers(
            OutboxProperties properties, Map<String, EventSerializer> serializerBeans) {
        return OutboxSerializers.resolve(
                serializerBeans,
                properties.getSerializer().getWriteFormat(),
                properties.getSerializer().getWriteFormatPerType());
    }

    @Bean
    @ConditionalOnMissingBean(OutboxEventPublisher.class)
    public OutboxEventPublisher outboxEventPublisher(
            EventStore store,
            OutboxSerializers serializers,
            Clock clock,
            TransactionContext txContext,
            OutboxProperties properties,
            PollerWakeHub wakeHub,
            ObjectProvider<OutboxTracer> tracerProvider,
            List<OutboxListener> listeners) {
        NoTransactionPolicy policy =
                mapNoTxPolicy(properties.getPublisher().getNoTransactionPolicy());
        // Publisher fires listener callbacks directly; the engine's ListenerRegistry will subsume
        // these when the engine starts — for now, aggregate all application-registered listeners.
        OutboxListener fanout = new FanOutListener(listeners);
        return new DefaultOutboxEventPublisher(
                store,
                serializers.write(),
                serializers.writePerType(),
                clock,
                txContext,
                policy,
                fanout,
                wakeHub,
                resolveTracer(tracerProvider),
                properties.getTracing().resolveLinkThreshold());
    }

    @Bean
    @ConditionalOnMissingBean
    public OutboxEngine outboxEngine(
            EventStore store,
            WorkerRegistry registry,
            EntityLocker locker,
            OutboxSerializers serializers,
            Clock clock,
            TransactionContext txContext,
            WorkerId workerId,
            OutboxProperties properties,
            PollerWakeHub wakeHub,
            ObjectProvider<OutboxAdmin> adminProvider,
            @OutboxDataSource ObjectProvider<DataSource> qualifiedDataSourceProvider,
            ObjectProvider<DataSource> dataSourceProvider,
            ObjectProvider<EventHandler<?>> handlerProvider,
            @Qualifier("outboxDefaultFailureHandler")
                    ObjectProvider<FailureHandler<?>> defaultFailureHandlerProvider,
            @Qualifier("outboxPerTypeFailureHandlers")
                    ObjectProvider<Map<String, FailureHandler<?>>> perTypeFailureHandlersProvider,
            ObjectProvider<PollStrategy> pollStrategyProvider,
            ObjectProvider<TaskDecorator> taskDecoratorProvider,
            ObjectProvider<OutboxTracer> tracerProvider,
            List<OutboxListener> listeners) {

        OutboxEngineBuilder builder =
                new OutboxEngineBuilder()
                        .eventStore(store)
                        .workerRegistry(registry)
                        .eventSerializer(serializers.write())
                        .additionalSerializers(
                                serializers.readOnly().toArray(EventSerializer[]::new))
                        .writeSerializerOverrides(serializers.writePerType())
                        .entityLocker(locker)
                        .clock(clock)
                        .transactionContext(txContext)
                        .noTransactionPolicy(
                                mapNoTxPolicy(properties.getPublisher().getNoTransactionPolicy()))
                        .workerIdSupplier(() -> workerId)
                        .wakeHub(wakeHub)
                        .tracer(resolveTracer(tracerProvider))
                        .tracingLinkThreshold(properties.getTracing().resolveLinkThreshold())
                        .maintenance(mapMaintenance(properties.getMaintenance()))
                        .retention(mapRetention(properties.getRetention()))
                        .dispatcher(mapDispatcher(properties.getDispatcher()));
        adminProvider.ifAvailable(builder::admin);

        // Thin merge (CONFIGURATION.md §Per-type override): user defaults overlay the library
        // defaults, and each per-type override overlays the resolved defaults field by field —
        // an override that sets only handler-pool-size keeps every other default intact.
        EventTypeConfig resolvedDefaults =
                mergeEventType(
                        properties.getEventTypes().getDefaults(), EventTypeConfig.defaults());
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

        java.util.List<EventHandler<?>> handlers = new java.util.ArrayList<>();
        handlerProvider.forEach(handlers::add);
        handlers.forEach(builder::handler);
        warnIfPgLockCanExhaustPool(
                properties,
                resolvedDefaults,
                handlers,
                qualifiedDataSourceProvider,
                dataSourceProvider);
        defaultFailureHandlerProvider.ifAvailable(builder::defaultFailureHandler);
        perTypeFailureHandlersProvider.ifAvailable(map -> map.forEach(builder::failureHandlerFor));
        pollStrategyProvider.ifAvailable(builder::pollStrategy);
        for (OutboxListener l : listeners) {
            builder.listener(l);
        }
        // Starter handles logging via application SLF4J config; avoid double-logging.
        builder.includeLoggingListener(false);

        // Wire handler executor factory per outbox.handler-executor.type. A user-provided
        // @Bean TaskDecorator wins; otherwise fall back to Spring's ContextPropagatingTaskDecorator
        // (which already propagates MDC / Observation / security context).
        TaskDecorator decorator =
                taskDecoratorProvider.getIfAvailable(ContextPropagatingTaskDecorator::new);
        builder.handlerExecutorFactory(
                switch (properties.getHandlerExecutor().getType()) {
                    case virtual -> HandlerExecutorFactory.virtual(decorator);
                    case platform -> HandlerExecutorFactory.platform(decorator);
                });

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

    /**
     * Resolves the tracing port (ADR-0023): a user-defined or auto-configured {@code OutboxTracer}
     * bean when present (see {@code spring.tracing} auto-configurations), {@link OutboxTracer#NOOP}
     * otherwise — publisher and engine share the same resolution.
     */
    private static OutboxTracer resolveTracer(ObjectProvider<OutboxTracer> tracerProvider) {
        return tracerProvider.getIfAvailable(() -> OutboxTracer.NOOP);
    }

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
                .abandonedHandlerGrace(m.getAbandonedHandlerGrace())
                .reclaimBatchSize(m.getReclaimBatchSize())
                .shutdownTimeout(m.getShutdownTimeout())
                .staleClaimThreshold(m.getStaleClaimThreshold())
                .staleClaimSweepInterval(m.getStaleClaimSweepInterval())
                .build();
    }

    private static RetentionConfig mapRetention(OutboxProperties.Retention r) {
        return RetentionConfig.builder()
                .archiveOlderThan(r.getArchiveOlderThan())
                .disabledOlderThan(r.getDisabledOlderThan())
                .batchSize(r.getBatchSize())
                .interval(r.getInterval())
                .build();
    }

    private static DispatcherConfig mapDispatcher(OutboxProperties.Dispatcher d) {
        return DispatcherConfig.builder()
                .unknownHandlerPolicy(
                        switch (d.getUnknownHandlerPolicy()) {
                            case SKIP -> UnknownHandlerPolicy.SKIP;
                            case DISABLE -> UnknownHandlerPolicy.DISABLE;
                            case FAIL -> UnknownHandlerPolicy.FAIL;
                        })
                .unknownHandlerRetryDelay(d.getUnknownHandlerRetryDelay())
                .lockBusyRetryDelay(d.getLockBusyRetryDelay())
                .dispatchRejectedRetryDelay(d.getDispatchRejectedRetryDelay())
                .finalizeBatching(d.isFinalizeBatching())
                .finalizeBatchMaxSize(d.getFinalizeBatchMaxSize())
                .build();
    }

    /**
     * The session-scoped advisory locker ({@code lock.type=postgres-advisory}) holds one pooled
     * connection for every concurrently-held lock (ADR-0012). If every handler thread can hold a
     * lock at once, the fleet can exhaust the shared HikariCP pool and deadlock against its own
     * handlers — warn at startup when the sums line up that way. The default lease locker ({@code
     * lock.type=postgres-lease}, ADR-0022) holds no connection during the handler, so the scenario
     * is structurally impossible there and the warning stays silent.
     */
    private void warnIfPgLockCanExhaustPool(
            OutboxProperties properties,
            EventTypeConfig resolvedDefaults,
            java.util.List<EventHandler<?>> handlers,
            ObjectProvider<DataSource> qualifiedDataSourceProvider,
            ObjectProvider<DataSource> dataSourceProvider) {
        if (properties.getLock().getType() != OutboxProperties.LockType.postgres_advisory) {
            return;
        }
        // Lenient resolution (ADR-0024): this is best-effort diagnostics — ambiguity must skip the
        // warning, never fail startup (e.g. a user EntityLocker bean with several DataSources).
        DataSource dataSource =
                OutboxDataSourceResolver.resolveIfUnambiguous(
                        qualifiedDataSourceProvider, dataSourceProvider);
        Integer maxPoolSize = hikariMaxPoolSize(dataSource);
        if (maxPoolSize == null) {
            return; // not HikariCP (or no DataSource) — nothing reliable to compare against
        }
        int totalHandlerThreads = 0;
        for (EventHandler<?> h : handlers) {
            OutboxProperties.EventType override =
                    properties.getEventTypes().getOverrides().get(h.eventType());
            EventTypeConfig cfg =
                    override != null
                            ? mergeEventType(override, resolvedDefaults)
                            : resolvedDefaults;
            totalHandlerThreads += cfg.handlerPoolSize();
        }
        pgLockPoolWarning(totalHandlerThreads, maxPoolSize).ifPresent(log::warn);
    }

    /** Package-private for tests. */
    static java.util.Optional<String> pgLockPoolWarning(int totalHandlerThreads, int maxPoolSize) {
        if (totalHandlerThreads < maxPoolSize) {
            return java.util.Optional.empty();
        }
        return java.util.Optional.of(
                "event-outboxer.lock.type=postgres-advisory holds one pooled connection per "
                        + "concurrently-held entity lock, and the total handler pool size ("
                        + totalHandlerThreads
                        + ") >= HikariCP maximum-pool-size ("
                        + maxPoolSize
                        + "): a saturated handler fleet can exhaust the connection pool and"
                        + " deadlock against its own handlers. Raise"
                        + " spring.datasource.hikari.maximum-pool-size, reduce the per-type handler"
                        + " pools, or switch to the lease locker"
                        + " (event-outboxer.lock.type=postgres-lease, ADR-0022), which holds no"
                        + " connection while the handler runs (ADR-0012).");
    }

    private static @Nullable Integer hikariMaxPoolSize(@Nullable DataSource dataSource) {
        if (dataSource == null
                || !ClassUtils.isPresent(
                        "com.zaxxer.hikari.HikariDataSource",
                        OutboxEngineAutoConfiguration.class.getClassLoader())) {
            return null;
        }
        return HikariSupport.maxPoolSize(dataSource);
    }

    /** Loaded only after the classpath check above — keeps Hikari a truly optional dependency. */
    private static final class HikariSupport {

        private HikariSupport() {}

        static @Nullable Integer maxPoolSize(DataSource ds) {
            return ds instanceof HikariDataSource hikari ? hikari.getMaximumPoolSize() : null;
        }
    }

    /**
     * Field-by-field overlay of a (possibly sparse) properties object onto a fully resolved base
     * config — the "thin merge" documented in CONFIGURATION.md. Package-private for tests.
     */
    static EventTypeConfig mergeEventType(OutboxProperties.EventType e, EventTypeConfig base) {
        return EventTypeConfig.builder()
                .pollMinInterval(
                        e.getPollMinInterval() != null
                                ? e.getPollMinInterval()
                                : base.pollMinInterval())
                .pollMaxInterval(
                        e.getPollMaxInterval() != null
                                ? e.getPollMaxInterval()
                                : base.pollMaxInterval())
                .pollMultiplier(
                        e.getPollMultiplier() != null
                                ? e.getPollMultiplier()
                                : base.pollMultiplier())
                .claimBatchSize(
                        e.getClaimBatchSize() != null
                                ? e.getClaimBatchSize()
                                : base.claimBatchSize())
                .handlerPoolSize(
                        e.getHandlerPoolSize() != null
                                ? e.getHandlerPoolSize()
                                : base.handlerPoolSize())
                .handlerQueueCapacity(
                        e.getHandlerQueueCapacity() != null
                                ? e.getHandlerQueueCapacity()
                                : base.handlerQueueCapacity())
                .handlerMaxRuntime(
                        e.getHandlerMaxRuntime() != null
                                ? e.getHandlerMaxRuntime()
                                : base.handlerMaxRuntime())
                .interruptStuckHandler(
                        e.getInterruptStuckHandler() != null
                                ? e.getInterruptStuckHandler()
                                : base.interruptStuckHandler())
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
        public void onEventPublished(EventPublishedInfo info) {
            for (OutboxListener l : delegates) {
                try {
                    l.onEventPublished(info);
                } catch (RuntimeException _) {
                    // isolation: one broken listener must not poison a publish
                }
            }
        }
    }
}
