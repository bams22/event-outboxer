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
import static org.awaitility.Awaitility.await;

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.core.config.EventTypeConfig;
import io.github.bams22.outboxer.core.config.MaintenanceConfig;
import io.github.bams22.outboxer.core.publish.NoTransactionPolicy;
import io.github.bams22.outboxer.spi.contracts.support.BinaryTestEventSerializer;
import io.github.bams22.outboxer.spi.contracts.support.BinaryTestPayload;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryWorkerRegistry;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof that the engine carries a binary payload verbatim (ADR-0025): publish → store →
 * claim → dispatch with a serializer whose output is deliberately not valid UTF-8, asserting the
 * handler receives an equal DTO.
 */
class BinaryPayloadDispatchTest {

    private OutboxEngine engine;

    @AfterEach
    void teardown() {
        if (engine != null && engine.isLifecycleActive()) {
            engine.stop(Duration.ofSeconds(2));
        }
    }

    @Test
    @DisplayName("publish → claim → dispatch round-trips a binary payload through the engine")
    void binaryPayloadRoundTripsThroughTheEngine() {
        InMemoryEventStore store = new InMemoryEventStore();
        AtomicReference<BinaryTestPayload> received = new AtomicReference<>();
        EventTypeConfig fast =
                EventTypeConfig.defaults().toBuilder()
                        .pollMinInterval(Duration.ofMillis(10))
                        .pollMaxInterval(Duration.ofMillis(50))
                        .pollMultiplier(1.1)
                        .build();
        MaintenanceConfig maintenance =
                MaintenanceConfig.builder()
                        .heartbeatInterval(Duration.ofMillis(200))
                        .deadThreshold(Duration.ofSeconds(5))
                        .orphanRecoveryInterval(Duration.ofSeconds(60))
                        .watchdogInterval(Duration.ofSeconds(1))
                        .reclaimBatchSize(10)
                        .shutdownTimeout(Duration.ofSeconds(2))
                        .staleClaimSweepInterval(Duration.ofMinutes(5))
                        .build();
        engine =
                new OutboxEngineBuilder()
                        .eventStore(store)
                        .workerRegistry(new InMemoryWorkerRegistry())
                        .eventSerializer(new BinaryTestEventSerializer())
                        .defaultEventTypeConfig(fast)
                        .maintenance(maintenance)
                        .noTransactionPolicy(NoTransactionPolicy.IGNORE)
                        .includeLoggingListener(false)
                        .handler(
                                new EventHandler<BinaryTestPayload>() {
                                    @Override
                                    public String eventType() {
                                        return "BINARY";
                                    }

                                    @Override
                                    public Class<BinaryTestPayload> payloadType() {
                                        return BinaryTestPayload.class;
                                    }

                                    @Override
                                    public EventOutcome handle(
                                            EventContext ctx, BinaryTestPayload payload) {
                                        received.set(payload);
                                        return EventOutcome.Success.INSTANCE;
                                    }
                                })
                        .build();
        engine.start();

        BinaryTestPayload original = new BinaryTestPayload("bin-раунд-трип", 4242);
        UUID id = engine.publisher().publish("BINARY", original);

        assertThat(
                        store.findById(id)
                                .map(
                                        e ->
                                                !e.payload().isText()
                                                        && e.payloadFormat().equals("test-binary"))
                                .orElse(true))
                .as("stored payload must be in the binary lane while pending")
                .isTrue();
        await().atMost(Duration.ofSeconds(10)).until(() -> store.findById(id).isEmpty());
        assertThat(received.get()).isEqualTo(original);
    }
}
