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
import io.github.bams22.outboxer.api.handle.builtin.FailureHandlers;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.observer.SerializationErrorInfo;
import io.github.bams22.outboxer.core.config.EventTypeConfig;
import io.github.bams22.outboxer.core.config.EventTypeConfigProvider;
import io.github.bams22.outboxer.core.support.StringEventSerializer;
import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.spi.EventSerializerRegistry;
import io.github.bams22.outboxer.spi.contracts.support.BinaryTestEventSerializer;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Deserialization routes by the {@code payloadFormat} stored at publish time, not by the write
 * serializer (ADR-0025) — the rolling-deploy / format-migration guarantee. An unknown format is a
 * recoverable failure: OUTBOX-203 through the failure chain, never an insta-DISABLE.
 */
class HandlerDispatcherFormatRoutingTest {

    private static final WorkerId WORKER = new WorkerId("format-routing-worker");

    private final InMemoryEventStore store = new InMemoryEventStore();
    private final AtomicReference<SerializationErrorInfo> serializationError =
            new AtomicReference<>();
    private final AtomicReference<String> handled = new AtomicReference<>();

    private HandlerDispatcher dispatcher(EventSerializerRegistry serializers) {
        EventHandler<String> handler =
                new EventHandler<String>() {
                    @Override
                    public String eventType() {
                        return "FMT";
                    }

                    @Override
                    public Class<String> payloadType() {
                        return String.class;
                    }

                    @Override
                    public EventOutcome handle(EventContext ctx, String payload) {
                        handled.set(payload);
                        return EventOutcome.Success.INSTANCE;
                    }
                };
        OutboxListener listener =
                new OutboxListener() {
                    @Override
                    public void onEventSerializationError(SerializationErrorInfo info) {
                        serializationError.set(info);
                    }
                };
        return new HandlerDispatcher(
                store,
                EntityLocker.NOOP,
                serializers,
                new EventHandlerResolver(List.of(handler)),
                new FailureHandlerResolver(Map.of(), FailureHandlers.defaults()),
                new InFlightRegistry(),
                listener,
                Clock.system(),
                WORKER,
                new EventTypeConfigProvider(EventTypeConfig.defaults(), Map.of()),
                DispatcherConfig.defaults());
    }

    private ClaimedEvent saveAndClaim(String payloadFormat) {
        UUID id = UUID.randomUUID();
        store.save(
                PendingEvent.builder()
                        .id(id)
                        .eventType("FMT")
                        .payload(SerializedPayload.ofText("\"old-format-payload\""))
                        .payloadFormat(payloadFormat)
                        .payloadClass("java.lang.String")
                        .priority((short) 0)
                        .runAt(Instant.now().minusSeconds(1))
                        .traceContext(Map.of())
                        .build());
        return store.claim(new ClaimRequest("FMT", WORKER, 10)).get(0);
    }

    @Test
    @DisplayName("an event written in yesterday's format routes to yesterday's serializer")
    void routesByStoredFormatNotByWriteSerializer() {
        // Rolling-deploy simulation: today's write serializer is binary, but the claimed event was
        // written by the string serializer — the registry must route the read to the old one.
        EventSerializerRegistry registry =
                EventSerializerRegistry.of(
                        List.of(new BinaryTestEventSerializer(), new StringEventSerializer()));
        ClaimedEvent claimed = saveAndClaim(StringEventSerializer.FORMAT);

        dispatcher(registry).dispatch(claimed);

        assertThat(handled.get()).isEqualTo("\"old-format-payload\"");
        assertThat(serializationError.get()).isNull();
        assertThat(store.findById(claimed.id())).as("processed events leave the outbox").isEmpty();
    }

    @Test
    @DisplayName("unknown stored format → OUTBOX-203 through the failure chain, event retried")
    void unknownFormatIsRecoverable() {
        EventSerializerRegistry registry =
                EventSerializerRegistry.of(List.of(new StringEventSerializer()));
        ClaimedEvent claimed = saveAndClaim("no-such-format");

        dispatcher(registry).dispatch(claimed);

        SerializationErrorInfo info = serializationError.get();
        assertThat(info).isNotNull();
        assertThat(info.payloadFormat()).isEqualTo("no-such-format");
        assertThat(info.storedPayloadClass()).isEqualTo("java.lang.String");
        assertThat(info.targetType()).isEqualTo("java.lang.String");
        assertThat(info.cause().getMessage()).contains("OUTBOX-203");

        assertThat(handled.get()).isNull();
        Event after = store.findById(claimed.id()).orElseThrow();
        assertThat(after.status())
                .as("first unknown-format failure schedules a retry, no insta-DISABLE")
                .isEqualTo(EventStatus.PENDING);
        assertThat(after.attempts()).isEqualTo(1);
    }
}
