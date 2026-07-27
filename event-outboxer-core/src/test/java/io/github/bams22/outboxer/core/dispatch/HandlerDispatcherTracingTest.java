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
import io.github.bams22.outboxer.core.config.EventTypeConfig;
import io.github.bams22.outboxer.core.config.EventTypeConfigProvider;
import io.github.bams22.outboxer.core.support.RecordingOutboxTracer;
import io.github.bams22.outboxer.core.support.StringEventSerializer;
import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;

/**
 * Consumer-span behaviour of {@link HandlerDispatcher} (ADR-0023): span per attempt around the
 * handler invocation, closed on the worker thread before finalize, exception recording, and
 * degradation when the tracer itself fails.
 */
class HandlerDispatcherTracingTest {

  private static final WorkerId WORKER = new WorkerId("tracing-test-worker");
  private static final Map<String, String> STORED_CONTEXT =
      Map.of("traceparent", "00-11111111111111111111111111111111-2222222222222222-01");

  /** Simple String handler delegating to a pluggable body. */
  private static final class TestHandler implements EventHandler<String> {

    private final BiFunction<EventContext, String, EventOutcome> body;

    private TestHandler(BiFunction<EventContext, String, EventOutcome> body) {
      this.body = body;
    }

    @Override
    public String eventType() {
      return "T";
    }

    @Override
    public Class<String> payloadType() {
      return String.class;
    }

    @Override
    public EventOutcome handle(EventContext ctx, String payload) {
      return body.apply(ctx, payload);
    }
  }

  private static HandlerDispatcher dispatcher(
      EventStore store, RecordingOutboxTracer tracer, EventHandler<?> handler) {
    return new HandlerDispatcher(
        store,
        EntityLocker.NOOP,
        new StringEventSerializer(),
        new EventHandlerResolver(List.of(handler)),
        new FailureHandlerResolver(Map.of(), FailureHandlers.defaults()),
        new InFlightRegistry(),
        new io.github.bams22.outboxer.api.observer.OutboxListener() {},
        Clock.system(),
        WORKER,
        new EventTypeConfigProvider(EventTypeConfig.defaults(), Map.of()),
        DispatcherConfig.defaults(),
        tracer);
  }

  private static ClaimedEvent saveAndClaim(InMemoryEventStore store) {
    store.save(
        PendingEvent.builder()
            .id(UUID.randomUUID())
            .eventType("T")
            .payload("p")
            .payloadClass("java.lang.String")
            .priority((short) 0)
            .runAt(Instant.now().minusSeconds(1))
            .traceContext(STORED_CONTEXT)
            .build());
    return store.claim(new ClaimRequest("T", WORKER, 10)).get(0);
  }

  @Test
  void consumerSpanWrapsHandlerAndCarriesStoredContext() {
    InMemoryEventStore store = new InMemoryEventStore();
    RecordingOutboxTracer tracer = new RecordingOutboxTracer();
    AtomicReference<Integer> closeCountDuringHandle = new AtomicReference<>();
    HandlerDispatcher dispatcher =
        dispatcher(
            store,
            tracer,
            new TestHandler(
                (ctx, payload) -> {
                  closeCountDuringHandle.set(tracer.processSpans.get(0).closeCount.get());
                  return EventOutcome.Success.INSTANCE;
                }));
    ClaimedEvent claimed = saveAndClaim(store);

    dispatcher.dispatch(claimed);

    assertThat(tracer.processSpans).hasSize(1);
    RecordingOutboxTracer.RecordedProcessSpan span = tracer.processSpans.get(0);
    assertThat(span.info.eventId()).isEqualTo(claimed.id());
    assertThat(span.info.eventType()).isEqualTo("T");
    assertThat(span.info.attempt()).isEqualTo(1);
    assertThat(span.info.workerId()).isEqualTo(WORKER);
    assertThat(span.info.storedContext()).isEqualTo(STORED_CONTEXT);
    assertThat(closeCountDuringHandle.get()).isZero(); // span open while the handler runs
    assertThat(span.closeCount).hasValue(1);
    assertThat(span.error).isNull();
  }

  @Test
  void eventContextStillReceivesRawStoredMap() {
    InMemoryEventStore store = new InMemoryEventStore();
    RecordingOutboxTracer tracer = new RecordingOutboxTracer();
    AtomicReference<Map<String, String>> seenByHandler = new AtomicReference<>();
    HandlerDispatcher dispatcher =
        dispatcher(
            store,
            tracer,
            new TestHandler(
                (ctx, payload) -> {
                  seenByHandler.set(ctx.traceContext());
                  return EventOutcome.Success.INSTANCE;
                }));

    dispatcher.dispatch(saveAndClaim(store));

    assertThat(seenByHandler.get()).isEqualTo(STORED_CONTEXT);
  }

  @Test
  void handlerExceptionIsRecordedOnSpanAndStillRoutedToRetry() {
    InMemoryEventStore store = new InMemoryEventStore();
    RecordingOutboxTracer tracer = new RecordingOutboxTracer();
    IllegalStateException boom = new IllegalStateException("handler exploded");
    HandlerDispatcher dispatcher =
        dispatcher(
            store,
            tracer,
            new TestHandler(
                (ctx, payload) -> {
                  throw boom;
                }));
    ClaimedEvent claimed = saveAndClaim(store);

    dispatcher.dispatch(claimed);

    RecordingOutboxTracer.RecordedProcessSpan span = tracer.processSpans.get(0);
    assertThat(span.error).isSameAs(boom);
    assertThat(span.closeCount).hasValue(1);
    // The failure chain still scheduled a retry — the event survived with a bumped attempt.
    assertThat(store.findById(claimed.id()).orElseThrow().attempts()).isEqualTo(1);
  }

  @Test
  void eachRetryAttemptGetsAFreshSpanWithSameStoredContext() {
    InMemoryEventStore store = new InMemoryEventStore();
    RecordingOutboxTracer tracer = new RecordingOutboxTracer();
    AtomicReference<EventOutcome> nextOutcome =
        new AtomicReference<>(new EventOutcome.Retry("first try fails", Duration.ZERO, null));
    HandlerDispatcher dispatcher =
        dispatcher(store, tracer, new TestHandler((ctx, payload) -> nextOutcome.get()));

    dispatcher.dispatch(saveAndClaim(store));
    nextOutcome.set(EventOutcome.Success.INSTANCE);
    // Retry with delayOverride ZERO is immediately claimable again.
    ClaimedEvent second = store.claim(new ClaimRequest("T", WORKER, 10)).get(0);
    dispatcher.dispatch(second);

    assertThat(tracer.processSpans).hasSize(2);
    assertThat(tracer.processSpans.get(0).info.attempt()).isEqualTo(1);
    assertThat(tracer.processSpans.get(1).info.attempt()).isEqualTo(2);
    assertThat(tracer.processSpans.get(1).info.storedContext()).isEqualTo(STORED_CONTEXT);
  }

  @Test
  void spanIsClosedBeforeFinalize() {
    RecordingOutboxTracer tracer = new RecordingOutboxTracer();
    AtomicReference<Integer> closeCountAtFinalize = new AtomicReference<>();
    InMemoryEventStore inner = new InMemoryEventStore();
    EventStore observing =
        new io.github.bams22.outboxer.core.support.ForwardingEventStore(inner) {
          @Override
          public boolean markProcessed(UUID id, WorkerId workerId, long claimedVersion) {
            closeCountAtFinalize.set(tracer.processSpans.get(0).closeCount.get());
            return super.markProcessed(id, workerId, claimedVersion);
          }
        };
    HandlerDispatcher dispatcher =
        dispatcher(observing, tracer, new TestHandler((ctx, payload) -> EventOutcome.Success.INSTANCE));

    dispatcher.dispatch(saveAndClaim(inner));

    assertThat(closeCountAtFinalize.get()).isEqualTo(1); // closed before markProcessed ran
  }

  @Test
  void throwingTracerStillProcessesTheEvent() {
    InMemoryEventStore store = new InMemoryEventStore();
    RecordingOutboxTracer tracer = new RecordingOutboxTracer();
    tracer.throwOnStart = true;
    AtomicReference<String> handled = new AtomicReference<>();
    HandlerDispatcher dispatcher =
        dispatcher(
            store,
            tracer,
            new TestHandler(
                (ctx, payload) -> {
                  handled.set(payload);
                  return EventOutcome.Success.INSTANCE;
                }));

    dispatcher.dispatch(saveAndClaim(store));

    assertThat(handled.get()).isEqualTo("p");
  }
}
