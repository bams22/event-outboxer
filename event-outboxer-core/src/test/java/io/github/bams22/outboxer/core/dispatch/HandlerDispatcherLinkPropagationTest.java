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
import io.github.bams22.outboxer.core.support.RecordingOutboxTracer;
import io.github.bams22.outboxer.core.support.StringEventSerializer;
import io.github.bams22.outboxer.core.tracing.TracePropagationMarker;
import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.domain.EventType;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.spi.EventSerializerRegistry;
import io.github.bams22.outboxer.spi.OutboxTracer;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * The handle side of the propagation decision (ADR-0023, 2026-08-28 amendment): the dispatcher
 * reads the {@code link} marker out of the stored carrier, hands the tracer the propagation mode
 * and a carrier without the marker, and keeps the marker away from the handler as well.
 */
class HandlerDispatcherLinkPropagationTest {

    private static final WorkerId WORKER = new WorkerId("link-test-worker");
    private static final Map<String, String> CARRIER =
            Map.of(
                    "traceparent", "00-11111111111111111111111111111111-2222222222222222-01",
                    "baggage", "tenant=acme");

    private final InMemoryEventStore store = new InMemoryEventStore();
    private final RecordingOutboxTracer tracer = new RecordingOutboxTracer();
    private final AtomicReference<Map<String, String>> seenByHandler = new AtomicReference<>();

    private HandlerDispatcher dispatcher() {
        EventHandler<String> handler =
                new EventHandler<>() {
                    @Override
                    public EventType<String> type() {
                        return EventType.of("T", String.class);
                    }

                    @Override
                    public EventOutcome handle(EventContext ctx, String payload) {
                        seenByHandler.set(ctx.traceContext());
                        return EventOutcome.success();
                    }
                };
        return HandlerDispatcher.builder()
                .store(store)
                .serializerRegistry(
                        EventSerializerRegistry.of(List.of(new StringEventSerializer())))
                .handlerResolver(new EventHandlerResolver(List.of(handler)))
                .workerId(WORKER)
                .tracer(tracer)
                .build();
    }

    private ClaimedEvent saveAndClaim(Map<String, String> traceContext) {
        store.save(
                PendingEvent.builder()
                        .id(UUID.randomUUID())
                        .eventType("T")
                        .payload(SerializedPayload.ofText("p"))
                        .payloadFormat(StringEventSerializer.FORMAT)
                        .payloadClass("java.lang.String")
                        .priority((short) 0)
                        .runAt(Instant.now().minusSeconds(1))
                        .traceContext(traceContext)
                        .build());
        return store.claim(new ClaimRequest("T", WORKER, 10)).get(0);
    }

    @Test
    void markedCarrierYieldsLinkPropagationWithTheMarkerStripped() {
        dispatcher().dispatch(saveAndClaim(TracePropagationMarker.markLinked(CARRIER)));

        OutboxTracer.ProcessSpanInfo info = tracer.processSpans.get(0).info;
        assertThat(info.propagation()).isEqualTo(OutboxTracer.Propagation.LINK);
        assertThat(info.storedContext()).isEqualTo(CARRIER);
        assertThat(seenByHandler.get()).isEqualTo(CARRIER);
    }

    @Test
    void unmarkedCarrierYieldsChildPropagationUnchanged() {
        dispatcher().dispatch(saveAndClaim(CARRIER));

        OutboxTracer.ProcessSpanInfo info = tracer.processSpans.get(0).info;
        assertThat(info.propagation()).isEqualTo(OutboxTracer.Propagation.CHILD);
        assertThat(info.storedContext()).isEqualTo(CARRIER);
        assertThat(seenByHandler.get()).isEqualTo(CARRIER);
    }

    @Test
    void unknownMarkerValueIsTreatedAsChildButStillStripped() {
        Map<String, String> odd =
                Map.of(
                        "traceparent",
                        CARRIER.get("traceparent"),
                        TracePropagationMarker.KEY,
                        "something-new");

        dispatcher().dispatch(saveAndClaim(odd));

        OutboxTracer.ProcessSpanInfo info = tracer.processSpans.get(0).info;
        assertThat(info.propagation()).isEqualTo(OutboxTracer.Propagation.CHILD);
        assertThat(info.storedContext()).containsOnlyKeys("traceparent");
        assertThat(seenByHandler.get()).containsOnlyKeys("traceparent");
    }
}
