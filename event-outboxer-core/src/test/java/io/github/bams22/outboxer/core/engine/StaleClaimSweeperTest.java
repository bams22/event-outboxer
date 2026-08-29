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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.core.config.EventTypeConfig;
import io.github.bams22.outboxer.core.config.MaintenanceConfig;
import io.github.bams22.outboxer.core.config.UnknownHandlerPolicy;
import io.github.bams22.outboxer.core.dispatch.DispatcherConfig;
import io.github.bams22.outboxer.core.publish.NoTransactionPolicy;
import io.github.bams22.outboxer.core.support.StringEventSerializer;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.EventType;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryWorkerRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The stale-claim safety net (ADR-0019 companion of ADR-0005): rows stranded in PROCESSING on a
 * LIVE worker — invisible to orphan recovery — are returned to PENDING by the DB-side sweeper, and
 * the widened in-flight bracket lets the watchdog catch hangs before the handler runs.
 */
class StaleClaimSweeperTest {

    /**
     * Real time plus a mutable offset, shared by the store and the engine: time flows normally
     * (watchdog/poller cadence works), while bumping the offset ages existing claims instantly.
     */
    private final AtomicReference<Duration> offset = new AtomicReference<>(Duration.ZERO);

    private final Clock clock = () -> Instant.now().plus(offset.get());

    private InMemoryEventStore store;
    private InMemoryWorkerRegistry registry;
    private OutboxEngine engine;

    @BeforeEach
    void setup() {
        store = new InMemoryEventStore(clock);
        registry = new InMemoryWorkerRegistry();
    }

    @AfterEach
    void teardown() {
        if (engine != null && engine.isLifecycleActive()) {
            engine.stop(Duration.ofSeconds(2));
        }
    }

    @Test
    @DisplayName("unknown-handler FAIL row is swept back to PENDING while the worker stays alive")
    void sweepsUnknownHandlerFailRow() {
        // handlerMaxRuntime tiny → derived threshold 2×500ms=1s; sweep every 500ms.
        engine =
                engineWith(
                                cfg -> cfg.handlerMaxRuntime(Duration.ofMillis(500)),
                                m ->
                                        m.staleClaimSweepInterval(Duration.ofMillis(500))
                                                .watchdogInterval(
                                                        Duration.ofSeconds(
                                                                60))) // watchdog out of the picture
                        .dispatcher(
                                DispatcherConfig.builder()
                                        .unknownHandlerPolicy(UnknownHandlerPolicy.FAIL)
                                        .unknownHandlerRetryDelay(Duration.ofMinutes(1))
                                        .lockBusyRetryDelay(Duration.ofSeconds(1))
                                        .dispatchRejectedRetryDelay(Duration.ofSeconds(1))
                                        .finalizeBatching(true)
                                        .finalizeBatchMaxSize(128)
                                        .build())
                        .handler(handler("KNOWN", (ctx, p) -> EventOutcome.success()))
                        .build();
        engine.start();

        // Simulate the artifact that unknown-handler FAIL (and any other tracked-by-nobody path)
        // produces: a row claimed into PROCESSING with no in-flight registration, on a worker that
        // keeps living. Orphan recovery never touches it; only the sweeper can.
        UUID id = UUID.randomUUID();
        store.save(
                PendingEvent.builder()
                        .id(id)
                        .eventType("ORPHANED_TYPE")
                        .payload(SerializedPayload.ofText("\"x\""))
                        .payloadFormat(StringEventSerializer.FORMAT)
                        .payloadClass("java.lang.String")
                        .priority((short) 0)
                        .runAt(clock.now().minusSeconds(1))
                        .traceContext(Map.of())
                        .build());
        store.claim(new ClaimRequest("ORPHANED_TYPE", new WorkerId("zombie"), 10));
        assertThat(store.findById(id).orElseThrow().status()).isEqualTo(EventStatus.PROCESSING);

        // Age the claim past the 1s threshold by advancing the shared clock.
        offset.updateAndGet(o -> o.plusSeconds(10));

        await().atMost(Duration.ofSeconds(5))
                .until(() -> store.findById(id).orElseThrow().status() == EventStatus.PENDING);
    }

    @Test
    @DisplayName("explicit staleClaimThreshold <= handlerMaxRuntime fails the build")
    void thresholdValidation() {
        assertThatThrownBy(
                        () ->
                                engineWith(
                                                cfg ->
                                                        cfg.handlerMaxRuntime(
                                                                Duration.ofMinutes(10)),
                                                m -> m.staleClaimThreshold(Duration.ofMinutes(5)))
                                        .handler(handler("T", (ctx, p) -> EventOutcome.success()))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("staleClaimThreshold");
    }

    @Test
    @DisplayName("derived threshold is 2 × the largest handlerMaxRuntime")
    void derivedThreshold() {
        EventTypeConfig small =
                EventTypeConfig.defaults().toBuilder()
                        .handlerMaxRuntime(Duration.ofMinutes(3))
                        .build();
        EventTypeConfig large =
                EventTypeConfig.defaults().toBuilder()
                        .handlerMaxRuntime(Duration.ofMinutes(7))
                        .build();

        assertThat(
                        OutboxEngineBuilder.resolveStaleClaimThreshold(
                                MaintenanceConfig.defaults(), List.of(small, large)))
                .isEqualTo(Duration.ofMinutes(14));
    }

    @Test
    @DisplayName(
            "widened in-flight bracket: watchdog reclaims a dispatch stuck in lock acquisition")
    void watchdogSeesLockAcquisitionHang() {
        AtomicInteger handled = new AtomicInteger();
        AtomicInteger lockCalls = new AtomicInteger();
        // First tryLock call hangs far beyond handlerMaxRuntime; subsequent calls succeed.
        EntityLocker hangingOnce =
                (key, ttl) -> {
                    if (lockCalls.incrementAndGet() == 1) {
                        sleepQuietly(10_000);
                        return Optional.empty();
                    }
                    return Optional.of(() -> {});
                };
        engine =
                engineWith(
                                cfg ->
                                        cfg.handlerMaxRuntime(Duration.ofMillis(300))
                                                .handlerPoolSize(2),
                                m -> m.watchdogInterval(Duration.ofMillis(200)))
                        .entityLocker(hangingOnce)
                        .handler(
                                new EventHandler<String>() {
                                    @Override
                                    public EventType<String> type() {
                                        return EventType.of("LOCKED", String.class);
                                    }

                                    @Override
                                    public String extractLockKey(String payload) {
                                        return "hot";
                                    }

                                    @Override
                                    public EventOutcome handle(EventContext ctx, String payload) {
                                        handled.incrementAndGet();
                                        return EventOutcome.success();
                                    }
                                })
                        .build();
        engine.start();

        UUID id = engine.publisher().publish(EventType.of("LOCKED", String.class), "payload");

        // Before the bracket widening the hang was invisible (registration happened after the
        // lock); now the watchdog force-reclaims it at handlerMaxRuntime and a retry succeeds.
        await().atMost(Duration.ofSeconds(8)).until(() -> store.findById(id).isEmpty());
        assertThat(handled).hasValue(1);
        assertThat(lockCalls.get()).isGreaterThanOrEqualTo(2);
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    private interface TypeCfg {
        EventTypeConfig.EventTypeConfigBuilder apply(EventTypeConfig.EventTypeConfigBuilder b);
    }

    private interface MaintCfg {
        MaintenanceConfig.MaintenanceConfigBuilder apply(
                MaintenanceConfig.MaintenanceConfigBuilder b);
    }

    private OutboxEngineBuilder engineWith(TypeCfg customizeType, MaintCfg customizeMaint) {
        EventTypeConfig typeCfg =
                customizeType
                        .apply(
                                EventTypeConfig.defaults().toBuilder()
                                        .pollMinInterval(Duration.ofMillis(10))
                                        .pollMaxInterval(Duration.ofMillis(50))
                                        .pollMultiplier(1.1))
                        .build();
        MaintenanceConfig maintenance =
                customizeMaint
                        .apply(
                                MaintenanceConfig.builder()
                                        .heartbeatInterval(Duration.ofMillis(200))
                                        .deadThreshold(Duration.ofSeconds(5))
                                        .orphanRecoveryInterval(Duration.ofSeconds(60))
                                        .watchdogInterval(Duration.ofSeconds(1))
                                        .abandonedHandlerGrace(Duration.ofSeconds(1))
                                        .reclaimBatchSize(10)
                                        .shutdownTimeout(Duration.ofSeconds(2))
                                        .staleClaimSweepInterval(Duration.ofMinutes(5)))
                        .build();
        return new OutboxEngineBuilder()
                .eventStore(store)
                .workerRegistry(registry)
                .eventSerializer(new StringEventSerializer())
                .clock(clock)
                .defaultEventTypeConfig(typeCfg)
                .maintenance(maintenance)
                .noTransactionPolicy(NoTransactionPolicy.IGNORE)
                .includeLoggingListener(false);
    }

    @FunctionalInterface
    private interface HandleFn {
        EventOutcome apply(EventContext ctx, String payload);
    }

    private static EventHandler<String> handler(String type, HandleFn fn) {
        return new EventHandler<String>() {
            @Override
            public EventType<String> type() {
                return EventType.of(type, String.class);
            }

            @Override
            public EventOutcome handle(EventContext ctx, String payload) {
                return fn.apply(ctx, payload);
            }
        };
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
