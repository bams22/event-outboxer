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
import io.github.bams22.outboxer.core.polling.PollerWaker;
import io.github.bams22.outboxer.core.support.ForwardingEventStore;
import io.github.bams22.outboxer.core.support.RecordingOutboxTracer;
import io.github.bams22.outboxer.core.support.StringEventSerializer;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.exception.EventStoreException;
import io.github.bams22.outboxer.domain.exception.PublishFailedException;
import io.github.bams22.outboxer.domain.exception.StorageException;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Producer-span behaviour of {@link DefaultOutboxEventPublisher} (ADR-0023): ambient capture into
 * the stored trace context, the explicit-override rule, coalescing tagging, error recording, and
 * degradation when the tracer itself fails.
 */
class DefaultOutboxEventPublisherTracingTest {

  private static final OutboxListener NOOP_LISTENER = new OutboxListener() {};

  private static DefaultOutboxEventPublisher publisher(EventStore store, RecordingOutboxTracer t) {
    return new DefaultOutboxEventPublisher(
        store,
        new StringEventSerializer(),
        Clock.system(),
        TransactionContext.alwaysActive(),
        NoTransactionPolicy.FAIL,
        NOOP_LISTENER,
        PollerWaker.NOOP,
        t);
  }

  @Test
  void capturedSpanContextIsStoredOnTheEvent() {
    InMemoryEventStore store = new InMemoryEventStore();
    RecordingOutboxTracer tracer = new RecordingOutboxTracer();

    UUID id = publisher(store, tracer).publish("T", "hello");

    assertThat(tracer.publishSpans).hasSize(1);
    RecordingOutboxTracer.RecordedPublishSpan span = tracer.publishSpans.get(0);
    assertThat(span.eventId).isEqualTo(id);
    assertThat(span.eventType).isEqualTo("T");
    assertThat(store.findById(id).orElseThrow().traceContext()).isEqualTo(span.context);
    assertThat(span.closeCount).hasValue(1);
    assertThat(span.error).isNull();
  }

  @Test
  void explicitTraceContextOverrideWins() {
    InMemoryEventStore store = new InMemoryEventStore();
    RecordingOutboxTracer tracer = new RecordingOutboxTracer();
    Map<String, String> explicit = Map.of("traceparent", "00-cafe-babe-01");

    UUID id =
        publisher(store, tracer)
            .publish("T", "hello", PublishOptions.builder().traceContext(explicit).build());

    assertThat(store.findById(id).orElseThrow().traceContext()).isEqualTo(explicit);
    // The producer span is still recorded in the caller's trace — only the stored map differs.
    assertThat(tracer.publishSpans).hasSize(1);
    assertThat(tracer.publishSpans.get(0).closeCount).hasValue(1);
  }

  @Test
  void storageFailureIsRecordedOnTheSpan() {
    EventStore failing =
        new ForwardingEventStore(new InMemoryEventStore()) {
          @Override
          public boolean save(PendingEvent event) {
            throw new EventStoreException("insert failed");
          }
        };
    RecordingOutboxTracer tracer = new RecordingOutboxTracer();

    assertThatThrownBy(() -> publisher(failing, tracer).publish("T", "hello"))
        .isInstanceOf(PublishFailedException.class);

    RecordingOutboxTracer.RecordedPublishSpan span = tracer.publishSpans.get(0);
    assertThat(span.error).isInstanceOf(StorageException.class);
    assertThat(span.closeCount).hasValue(1);
  }

  @Test
  void coalescedPublishTagsSpanAndKeepsExistingContext() {
    InMemoryEventStore store = new InMemoryEventStore();
    RecordingOutboxTracer tracer = new RecordingOutboxTracer();
    DefaultOutboxEventPublisher publisher = publisher(store, tracer);
    PublishOptions keyed = PublishOptions.builder().dedupKey("order-1").build();

    UUID first = publisher.publish("SYNC", "v1", keyed);
    UUID second = publisher.publish("SYNC", "v2", keyed);

    assertThat(second).isEqualTo(first);
    assertThat(tracer.publishSpans).hasSize(2);
    RecordingOutboxTracer.RecordedPublishSpan coalescedSpan = tracer.publishSpans.get(1);
    assertThat(coalescedSpan.coalescedInto).isEqualTo(first);
    assertThat(coalescedSpan.error).isNull();
    assertThat(coalescedSpan.closeCount).hasValue(1);
    // The surviving row keeps the FIRST publish's context — the second capture is discarded.
    assertThat(store.findById(first).orElseThrow().traceContext())
        .isEqualTo(tracer.publishSpans.get(0).context);
  }

  @Test
  void publishAllCreatesOneSpanPerRequestWithDistinctContexts() {
    InMemoryEventStore store = new InMemoryEventStore();
    RecordingOutboxTracer tracer = new RecordingOutboxTracer();
    PublishOptions keyed = PublishOptions.builder().dedupKey("k").build();

    List<UUID> ids =
        publisher(store, tracer)
            .publishAll(
                List.of(
                    new PublishRequest("A", "a1", keyed),
                    new PublishRequest("A", "plain1", null),
                    new PublishRequest("B", "plain2", null)));

    assertThat(tracer.publishSpans).hasSize(3);
    assertThat(tracer.publishSpans)
        .allSatisfy(span -> assertThat(span.closeCount).hasValue(1));
    // Each row stores its own span's context, including the batch-path rows saved via saveAll.
    assertThat(store.findById(ids.get(0)).orElseThrow().traceContext())
        .isEqualTo(tracer.publishSpans.get(0).context);
    assertThat(store.findById(ids.get(1)).orElseThrow().traceContext())
        .isEqualTo(tracer.publishSpans.get(1).context);
    assertThat(store.findById(ids.get(2)).orElseThrow().traceContext())
        .isEqualTo(tracer.publishSpans.get(2).context);
    assertThat(tracer.publishSpans.get(0).context)
        .isNotEqualTo(tracer.publishSpans.get(1).context);
  }

  @Test
  void publishAllStorageFailureMarksOpenBatchSpans() {
    EventStore failing =
        new ForwardingEventStore(new InMemoryEventStore()) {
          @Override
          public void saveAll(List<PendingEvent> events) {
            throw new EventStoreException("batch insert failed");
          }
        };
    RecordingOutboxTracer tracer = new RecordingOutboxTracer();

    assertThatThrownBy(
            () ->
                publisher(failing, tracer)
                    .publishAll(
                        List.of(
                            new PublishRequest("A", "a1", null),
                            new PublishRequest("B", "b1", null))))
        .isInstanceOf(PublishFailedException.class);

    assertThat(tracer.publishSpans).hasSize(2);
    assertThat(tracer.publishSpans)
        .allSatisfy(
            span -> {
              assertThat(span.error).isInstanceOf(StorageException.class);
              assertThat(span.closeCount).hasValue(1);
            });
  }

  @Test
  void throwingTracerDoesNotBreakPublish() {
    InMemoryEventStore store = new InMemoryEventStore();
    RecordingOutboxTracer tracer = new RecordingOutboxTracer();
    tracer.throwOnStart = true;

    UUID id = publisher(store, tracer).publish("T", "hello");

    assertThat(store.findById(id)).isPresent();
    assertThat(store.findById(id).orElseThrow().traceContext()).isEmpty();
  }
}
