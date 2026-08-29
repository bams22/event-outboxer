/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.publish;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.publish.PublishOptions;
import io.github.bams22.outboxer.api.publish.PublishRequest;
import io.github.bams22.outboxer.core.support.RecordingOutboxTracer;
import io.github.bams22.outboxer.core.support.StringEventSerializer;
import io.github.bams22.outboxer.core.tracing.TracePropagationMarker;
import io.github.bams22.outboxer.domain.EventType;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.OutboxTracer;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The publish-time propagation decision (ADR-0023, 2026-08-28 amendment): an event scheduled
 * further ahead than the link threshold is stored with the {@code link} marker and its producer
 * span is told; everything else keeps the plain carrier.
 */
class DefaultOutboxEventPublisherLinkThresholdTest {

    private static final OutboxListener NOOP_LISTENER = new OutboxListener() {};

    private static DefaultOutboxEventPublisher publisher(
            EventStore store, RecordingOutboxTracer tracer, Duration threshold) {
        return publisher(store, tracer, OutboxTracer.Propagation.LINK, threshold);
    }

    private static DefaultOutboxEventPublisher publisher(
            EventStore store,
            RecordingOutboxTracer tracer,
            OutboxTracer.Propagation propagation,
            Duration threshold) {
        return DefaultOutboxEventPublisher.builder()
                .store(store)
                .serializer(new StringEventSerializer())
                .noTransactionPolicy(NoTransactionPolicy.FAIL)
                .tracer(tracer)
                .deferredPropagation(propagation)
                .linkThreshold(threshold)
                .build();
    }

    private static Instant inTwoDays() {
        return Instant.now().plus(Duration.ofDays(2));
    }

    @Test
    void eventScheduledBeyondTheThresholdIsMarkedAndTheProducerSpanIsTold() {
        InMemoryEventStore store = new InMemoryEventStore();
        RecordingOutboxTracer tracer = new RecordingOutboxTracer();

        UUID id =
                publisher(store, tracer, Duration.ofMinutes(1))
                        .publish(EventType.of("T", String.class), "hello", inTwoDays());

        RecordingOutboxTracer.RecordedPublishSpan span = tracer.publishSpans.get(0);
        assertThat(span.linked).isTrue();
        Map<String, String> stored = store.findById(id).orElseThrow().traceContext();
        assertThat(stored)
                .containsAllEntriesOf(span.context)
                .containsEntry(TracePropagationMarker.KEY, "link");
        assertThat(TracePropagationMarker.propagationOf(stored))
                .isEqualTo(OutboxTracer.Propagation.LINK);
    }

    @Test
    void eventWithinTheThresholdKeepsThePlainCarrier() {
        InMemoryEventStore store = new InMemoryEventStore();
        RecordingOutboxTracer tracer = new RecordingOutboxTracer();

        UUID id =
                publisher(store, tracer, Duration.ofMinutes(1))
                        .publish(
                                EventType.of("T", String.class),
                                "hello",
                                Instant.now().plusSeconds(10));

        assertThat(tracer.publishSpans.get(0).linked).isFalse();
        assertThat(store.findById(id).orElseThrow().traceContext())
                .isEqualTo(tracer.publishSpans.get(0).context);
    }

    @Test
    void immediateEventNeverLinks() {
        InMemoryEventStore store = new InMemoryEventStore();
        RecordingOutboxTracer tracer = new RecordingOutboxTracer();

        UUID id =
                publisher(store, tracer, Duration.ZERO)
                        .publish(EventType.of("T", String.class), "hello");

        assertThat(tracer.publishSpans.get(0).linked).isFalse();
        assertThat(store.findById(id).orElseThrow().traceContext())
                .doesNotContainKey(TracePropagationMarker.KEY);
    }

    @Test
    void zeroThresholdLinksEveryFutureRunAt() {
        InMemoryEventStore store = new InMemoryEventStore();
        RecordingOutboxTracer tracer = new RecordingOutboxTracer();

        UUID id =
                publisher(store, tracer, Duration.ZERO)
                        .publish(
                                EventType.of("T", String.class),
                                "hello",
                                Instant.now().plusSeconds(30));

        assertThat(tracer.publishSpans.get(0).linked).isTrue();
        assertThat(store.findById(id).orElseThrow().traceContext())
                .containsEntry(TracePropagationMarker.KEY, "link");
    }

    @Test
    void childPropagationDisablesTheRule() {
        InMemoryEventStore store = new InMemoryEventStore();
        RecordingOutboxTracer tracer = new RecordingOutboxTracer();

        UUID id =
                publisher(store, tracer, OutboxTracer.Propagation.CHILD, Duration.ofMinutes(1))
                        .publish(EventType.of("T", String.class), "hello", inTwoDays());

        assertThat(tracer.publishSpans.get(0).linked).isFalse();
        assertThat(store.findById(id).orElseThrow().traceContext())
                .doesNotContainKey(TracePropagationMarker.KEY);
    }

    @Test
    void defaultThresholdIsOneMinute() {
        InMemoryEventStore store = new InMemoryEventStore();
        RecordingOutboxTracer tracer = new RecordingOutboxTracer();
        DefaultOutboxEventPublisher publisher =
                DefaultOutboxEventPublisher.builder()
                        .store(store)
                        .serializer(new StringEventSerializer())
                        .tracer(tracer)
                        .build();

        UUID soon =
                publisher.publish(
                        EventType.of("T", String.class), "a", Instant.now().plusSeconds(30));
        UUID later =
                publisher.publish(
                        EventType.of("T", String.class),
                        "b",
                        Instant.now().plus(Duration.ofMinutes(2)));

        assertThat(DefaultOutboxEventPublisher.DEFAULT_LINK_THRESHOLD)
                .isEqualTo(Duration.ofMinutes(1));
        assertThat(DefaultOutboxEventPublisher.DEFAULT_DEFERRED_PROPAGATION)
                .isEqualTo(OutboxTracer.Propagation.LINK);
        assertThat(store.findById(soon).orElseThrow().traceContext())
                .doesNotContainKey(TracePropagationMarker.KEY);
        assertThat(store.findById(later).orElseThrow().traceContext())
                .containsEntry(TracePropagationMarker.KEY, "link");
    }

    @Test
    void explicitTraceContextOverrideIsMarkedToo() {
        InMemoryEventStore store = new InMemoryEventStore();
        RecordingOutboxTracer tracer = new RecordingOutboxTracer();
        Map<String, String> explicit =
                Map.of("traceparent", "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01");

        UUID id =
                publisher(store, tracer, Duration.ofMinutes(1))
                        .publish(
                                EventType.of("T", String.class),
                                "hello",
                                PublishOptions.builder()
                                        .runAt(inTwoDays())
                                        .traceContext(explicit)
                                        .build());

        Map<String, String> expected = new HashMap<>(explicit);
        expected.put(TracePropagationMarker.KEY, "link");
        assertThat(store.findById(id).orElseThrow().traceContext()).isEqualTo(expected);
        assertThat(tracer.publishSpans.get(0).linked).isTrue();
    }

    @Test
    void emptyCarrierStaysEmptyEvenWhenDeferred() {
        InMemoryEventStore store = new InMemoryEventStore();
        DefaultOutboxEventPublisher publisher =
                DefaultOutboxEventPublisher.builder()
                        .store(store)
                        .serializer(new StringEventSerializer())
                        .linkThreshold(Duration.ofMinutes(1))
                        .build();

        UUID id = publisher.publish(EventType.of("T", String.class), "hello", inTwoDays());

        // No stored context means nothing to link to; a marker-only map would be noise.
        assertThat(store.findById(id).orElseThrow().traceContext()).isEmpty();
    }

    @Test
    void publishAllDecidesPerRequest() {
        InMemoryEventStore store = new InMemoryEventStore();
        RecordingOutboxTracer tracer = new RecordingOutboxTracer();

        List<UUID> ids =
                publisher(store, tracer, Duration.ofMinutes(1))
                        .publishAll(
                                List.of(
                                        new PublishRequest<>(
                                                EventType.of("T", String.class),
                                                "now",
                                                PublishOptions.defaults()),
                                        new PublishRequest<>(
                                                EventType.of("T", String.class),
                                                "later",
                                                PublishOptions.builder()
                                                        .runAt(inTwoDays())
                                                        .build()),
                                        new PublishRequest<>(
                                                EventType.of("T", String.class),
                                                "later-coalesced",
                                                PublishOptions.builder()
                                                        .runAt(inTwoDays())
                                                        .dedupKey("k")
                                                        .build())));

        assertThat(store.findById(ids.get(0)).orElseThrow().traceContext())
                .doesNotContainKey(TracePropagationMarker.KEY);
        assertThat(store.findById(ids.get(1)).orElseThrow().traceContext())
                .containsEntry(TracePropagationMarker.KEY, "link");
        assertThat(store.findById(ids.get(2)).orElseThrow().traceContext())
                .containsEntry(TracePropagationMarker.KEY, "link");
        assertThat(tracer.publishSpans)
                .extracting(s -> s.linked)
                .containsExactly(false, true, true);
    }

    @Test
    void negativeThresholdIsRejected() {
        assertThatThrownBy(
                        () ->
                                publisher(
                                        new InMemoryEventStore(),
                                        new RecordingOutboxTracer(),
                                        Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("linkThreshold");
    }
}
