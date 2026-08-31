/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.dispatch;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.observer.StorageErrorInfo;
import io.github.bams22.outboxer.core.support.ForwardingEventStore;
import io.github.bams22.outboxer.core.support.StringEventSerializer;
import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.EventType;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.spi.EventSerializerRegistry;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Finalize-failure recovery emits {@code onStorageError}: a failed finalize reports {@code
 * "finalize"} and releases the row back to {@code PENDING}; when the recovery release fails too, a
 * second {@code "release"} error is reported and the row stays {@code PROCESSING} for crash
 * recovery.
 */
class HandlerDispatcherFinalizeFailureTest {

    private static final WorkerId WORKER = new WorkerId("finalize-test-worker");
    private static final String TYPE = "FIN";

    private final CopyOnWriteArrayList<StorageErrorInfo> storageErrors =
            new CopyOnWriteArrayList<>();

    private HandlerDispatcher dispatcher(EventStore store) {
        EventHandler<String> handler =
                new EventHandler<String>() {
                    @Override
                    public EventType<String> type() {
                        return EventType.of(TYPE, String.class);
                    }

                    @Override
                    public EventOutcome handle(EventContext ctx, String payload) {
                        return EventOutcome.success();
                    }
                };
        OutboxListener listener =
                new OutboxListener() {
                    @Override
                    public void onStorageError(StorageErrorInfo info) {
                        storageErrors.add(info);
                    }
                };
        return HandlerDispatcher.builder()
                .store(store)
                .serializerRegistry(
                        EventSerializerRegistry.of(List.of(new StringEventSerializer())))
                .handlerResolver(new EventHandlerResolver(List.of(handler)))
                .listener(listener)
                .workerId(WORKER)
                .build();
    }

    private static ClaimedEvent saveAndClaim(EventStore store) {
        store.save(
                PendingEvent.builder()
                        .id(UUID.randomUUID())
                        .eventType(TYPE)
                        .payload(SerializedPayload.ofText("\"p\""))
                        .payloadFormat(StringEventSerializer.FORMAT)
                        .payloadClass("java.lang.String")
                        .priority((short) 0)
                        .runAt(Instant.now().minusSeconds(1))
                        .traceContext(Map.of())
                        .build());
        return store.claim(new ClaimRequest(TYPE, WORKER, 10)).get(0);
    }

    @Test
    @DisplayName(
            "failed finalize → onStorageError(\"finalize\") and the row is released to PENDING")
    void finalizeFailureReportsAndReleases() {
        EventStore store =
                new ForwardingEventStore(new InMemoryEventStore()) {
                    @Override
                    public boolean markProcessed(UUID id, WorkerId workerId, long claimedVersion) {
                        throw new IllegalStateException("markProcessed down (simulated)");
                    }
                };
        ClaimedEvent claimed = saveAndClaim(store);

        dispatcher(store).dispatch(claimed);

        assertThat(storageErrors)
                .singleElement()
                .satisfies(
                        info -> {
                            assertThat(info.operation()).isEqualTo("finalize");
                            assertThat(info.cause()).hasMessageContaining("markProcessed down");
                        });
        Event after = store.findById(claimed.id()).orElseThrow();
        assertThat(after.status()).isEqualTo(EventStatus.PENDING);
        assertThat(after.claimedBy()).isNull();
    }

    @Test
    @DisplayName("failed finalize whose recovery release also fails reports both operations")
    void releaseFailureAfterFinalizeFailureReportsBoth() {
        EventStore store =
                new ForwardingEventStore(new InMemoryEventStore()) {
                    @Override
                    public boolean markProcessed(UUID id, WorkerId workerId, long claimedVersion) {
                        throw new IllegalStateException("markProcessed down (simulated)");
                    }

                    @Override
                    public boolean release(
                            UUID id,
                            WorkerId workerId,
                            long claimedVersion,
                            String reason,
                            Instant runAt) {
                        throw new IllegalStateException("release down (simulated)");
                    }
                };
        ClaimedEvent claimed = saveAndClaim(store);

        dispatcher(store).dispatch(claimed);

        assertThat(storageErrors)
                .extracting(StorageErrorInfo::operation)
                .containsExactly("finalize", "release");
        Event after = store.findById(claimed.id()).orElseThrow();
        assertThat(after.status())
                .as("storage fully down — the row stays PROCESSING for crash recovery")
                .isEqualTo(EventStatus.PROCESSING);
    }
}
