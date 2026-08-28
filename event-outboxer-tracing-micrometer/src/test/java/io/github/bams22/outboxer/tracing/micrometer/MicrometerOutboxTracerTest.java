/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.tracing.micrometer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.OutboxTracer;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.tck.TestObservationRegistry;
import io.micrometer.observation.tck.TestObservationRegistryAssert;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingReceiverTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingSenderTracingObservationHandler;
import io.micrometer.tracing.propagation.Propagator;
import io.micrometer.tracing.test.simple.SimpleSpan;
import io.micrometer.tracing.test.simple.SimpleTracer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Span model and handle semantics of the adapter, driven through the same {@code
 * ObservationHandler}s Spring Boot registers (ADR-0023).
 */
class MicrometerOutboxTracerTest {

    private static final WorkerId WORKER = new WorkerId("micrometer-test-worker");

    private final SimpleTracer tracer = new SimpleTracer();
    private final RecordingPropagator propagator = new RecordingPropagator(tracer);
    private final TestObservationRegistry registry = TestObservationRegistry.create();
    private final MicrometerOutboxTracer outboxTracer =
            new MicrometerOutboxTracer(registry, tracer);

    /**
     * The handler set Spring Boot registers whenever a {@code Tracer} and a {@code Propagator} bean
     * exist — the adapter delegates span creation, kind, parent extraction and carrier injection to
     * them. {@code DefaultTracingObservationHandler} is included, and ordered last exactly as
     * Boot's {@code ObservationHandlerGrouping} orders it: it matches every context, so if it ever
     * won the first-matching composite the carrier would silently stay empty and span names would
     * be mangled through {@code SpanNameUtil}. Keeping it here means every assertion below also
     * asserts that the propagating handlers still win.
     */
    @BeforeEach
    void registerTracingHandlers() {
        registry.observationConfig().observationHandler(tracingHandlers(true));
    }

    /**
     * Boot's tracing group, optionally led by the adapter's own receiver handler — the position the
     * starter gives it (ADR-0023, 2026-08-28 amendment).
     */
    private ObservationHandler<?> tracingHandlers(boolean withOutboxReceiverHandler) {
        List<ObservationHandler<?>> handlers = new ArrayList<>();
        if (withOutboxReceiverHandler) {
            handlers.add(new OutboxReceiverTracingObservationHandler(tracer, propagator));
        }
        handlers.add(new PropagatingReceiverTracingObservationHandler<>(tracer, propagator));
        handlers.add(new PropagatingSenderTracingObservationHandler<>(tracer, propagator));
        handlers.add(new DefaultTracingObservationHandler(tracer));
        return new ObservationHandler.FirstMatchingCompositeObservationHandler(handlers);
    }

    /**
     * Test double for the configured propagator: injects a synthetic {@code traceparent} from the
     * span context and records every inject/extract interaction.
     */
    private static final class RecordingPropagator implements Propagator {

        private final SimpleTracer tracer;
        final List<TraceContext> injectedContexts = new ArrayList<>();
        final List<Object> extractedCarriers = new ArrayList<>();

        private RecordingPropagator(SimpleTracer tracer) {
            this.tracer = tracer;
        }

        @Override
        public List<String> fields() {
            return List.of("traceparent");
        }

        @Override
        public <C> void inject(TraceContext context, C carrier, Setter<C> setter) {
            injectedContexts.add(context);
            setter.set(
                    carrier,
                    "traceparent",
                    "00-" + context.traceId() + "-" + context.spanId() + "-01");
        }

        @Override
        public <C> Span.Builder extract(C carrier, Getter<C> getter) {
            extractedCarriers.add(carrier);
            Span.Builder builder = tracer.spanBuilder();
            String traceparent = getter.get(carrier, "traceparent");
            if (traceparent != null) {
                String[] parts = traceparent.split("-");
                builder.setParent(
                        tracer.traceContextBuilder()
                                .traceId(parts[1])
                                .spanId(parts[2])
                                .sampled(true)
                                .build());
            }
            return builder;
        }
    }

    private static OutboxTracer.ProcessSpanInfo processInfo(Map<String, String> stored) {
        return new OutboxTracer.ProcessSpanInfo(UUID.randomUUID(), "T", 3, WORKER, stored);
    }

    private static OutboxTracer.ProcessSpanInfo linkedInfo(Map<String, String> stored) {
        return new OutboxTracer.ProcessSpanInfo(
                UUID.randomUUID(), "T", 3, WORKER, stored, OutboxTracer.Propagation.LINK);
    }

    @Test
    void publishSpanInjectsItsContextAndCarriesTags() {
        UUID id = UUID.randomUUID();

        OutboxTracer.PublishSpan span = outboxTracer.startPublishSpan(id, "T");
        Map<String, String> stored = span.contextToStore();
        span.close();

        SimpleSpan finished = tracer.onlySpan();
        assertThat(finished.getName()).isEqualTo("outbox publish T");
        assertThat(finished.getKind()).isEqualTo(Span.Kind.PRODUCER);
        assertThat(finished.getTags())
                .containsEntry("messaging.system", "event_outboxer")
                .containsEntry("messaging.operation.type", "send")
                .containsEntry("messaging.destination.name", "T")
                .containsEntry("messaging.message.id", id.toString());
        assertThat(propagator.injectedContexts).hasSize(1);
        assertThat(stored)
                .containsEntry(
                        "traceparent",
                        "00-"
                                + finished.context().traceId()
                                + "-"
                                + finished.context().spanId()
                                + "-01");
    }

    @Test
    void publishObservationIsNamedForMetricsAndKeepsIdsOffTheTimer() {
        UUID id = UUID.randomUUID();

        outboxTracer.startPublishSpan(id, "T").close();

        TestObservationRegistryAssert.assertThat(registry)
                .hasObservationWithNameEqualTo(outboxTracer.publishObservationName())
                .that()
                .hasBeenStarted()
                .hasBeenStopped()
                .hasContextualNameEqualTo("outbox publish T")
                .hasLowCardinalityKeyValue("messaging.destination.name", "T")
                .hasHighCardinalityKeyValue("messaging.message.id", id.toString());
    }

    @Test
    void publishSpanNeverBecomesCurrentOnTheCallerThread() {
        OutboxTracer.PublishSpan span = outboxTracer.startPublishSpan(UUID.randomUUID(), "T");

        assertThat(registry.getCurrentObservation()).isNull();
        assertThat(tracer.currentSpan()).isNull();

        span.close();
    }

    @Test
    void processSpanExtractsStoredCarrierAndBecomesCurrent() {
        Map<String, String> stored =
                Map.of("traceparent", "00-11111111111111111111111111111111-2222222222222222-01");

        OutboxTracer.ProcessSpan span = outboxTracer.startProcessSpan(processInfo(stored));
        Span current = tracer.currentSpan();
        // The current observation is what ContextSnapshot carries across a handler's thread hop.
        assertThat(registry.getCurrentObservation()).isNotNull();
        span.close();

        assertThat(propagator.extractedCarriers).containsExactly(stored);
        SimpleSpan finished = tracer.onlySpan();
        assertThat(current).isNotNull();
        assertThat(tracer.currentSpan()).isNull(); // scope closed with the handle
        assertThat(registry.getCurrentObservation()).isNull();
        assertThat(finished.getName()).isEqualTo("outbox process T");
        assertThat(finished.getKind()).isEqualTo(Span.Kind.CONSUMER);
        assertThat(finished.getTags())
                .containsEntry("messaging.operation.type", "process")
                .containsEntry("event_outboxer.attempt", "3")
                .containsEntry("event_outboxer.worker.id", WORKER.value());
    }

    @Test
    void errorIsDelegatedToTheSpan() {
        IllegalStateException boom = new IllegalStateException("handler exploded");

        OutboxTracer.ProcessSpan span = outboxTracer.startProcessSpan(processInfo(Map.of()));
        span.error(boom);
        span.close();

        assertThat(tracer.onlySpan().getError()).isSameAs(boom);
    }

    @Test
    void coalescedAddsTheCoalescedIntoTag() {
        UUID existing = UUID.randomUUID();

        OutboxTracer.PublishSpan span = outboxTracer.startPublishSpan(UUID.randomUUID(), "T");
        span.coalesced(existing);
        span.close();

        assertThat(tracer.onlySpan().getTags())
                .containsEntry("event_outboxer.coalesced_into", existing.toString());
    }

    @Test
    void closeIsIdempotent() {
        OutboxTracer.PublishSpan publish = outboxTracer.startPublishSpan(UUID.randomUUID(), "T");
        publish.close();
        publish.close();
        OutboxTracer.ProcessSpan process = outboxTracer.startProcessSpan(processInfo(Map.of()));
        process.close();
        process.close();

        assertThat(tracer.getSpans()).hasSize(2);
    }

    @Test
    void noopRegistryDegradesToAnUntracedHop() {
        MicrometerOutboxTracer noop = new MicrometerOutboxTracer(ObservationRegistry.NOOP, tracer);

        OutboxTracer.PublishSpan publish = noop.startPublishSpan(UUID.randomUUID(), "T");
        assertThat(publish.contextToStore()).isEmpty();
        publish.close();
        noop.startProcessSpan(processInfo(Map.of())).close();

        assertThat(tracer.getSpans()).isEmpty();
    }

    /**
     * The observations feed meters, so their names must follow the same {@code
     * event-outboxer.metrics.prefix} slot every other meter of the library honours.
     */
    @Test
    void observationNamesFollowTheConfiguredPrefix() {
        MicrometerOutboxTracer prefixed =
                new MicrometerOutboxTracer(registry, tracer, "myapp.outbox");

        assertThat(prefixed.publishObservationName()).isEqualTo("myapp.outbox.publish");
        assertThat(prefixed.processObservationName()).isEqualTo("myapp.outbox.process");

        prefixed.startPublishSpan(UUID.randomUUID(), "T").close();

        TestObservationRegistryAssert.assertThat(registry)
                .hasObservationWithNameEqualTo("myapp.outbox.publish");
    }

    /**
     * Detaching the tracer's current span is not enough: {@code createNotStarted} captures the
     * thread's current observation as the parent before that. A worker carrying a leaked scope must
     * not end up parenting the consumer observation either (ADR-0023).
     */
    @Test
    void processObservationDoesNotAdoptAnAmbientObservationAsParent() {
        Observation ambient = Observation.createNotStarted("ambient", registry).start();
        try (Observation.Scope ignored = ambient.openScope()) {
            outboxTracer.startProcessSpan(processInfo(Map.of())).close();
        }
        ambient.stop();

        TestObservationRegistryAssert.assertThat(registry)
                .hasObservationWithNameEqualTo(outboxTracer.processObservationName())
                .that()
                .doesNotHaveParentObservation();
    }

    /**
     * The publish side is the mirror image: the caller's observation is exactly the parent the
     * producer span should hang from, so nothing is detached there.
     */
    @Test
    void publishObservationKeepsTheCallersObservationAsParent() {
        Observation caller = Observation.createNotStarted("business-op", registry).start();
        try (Observation.Scope ignored = caller.openScope()) {
            outboxTracer.startPublishSpan(UUID.randomUUID(), "T").close();
        }
        caller.stop();

        TestObservationRegistryAssert.assertThat(registry)
                .hasObservationWithNameEqualTo(outboxTracer.publishObservationName())
                .that()
                .hasParentObservationEqualTo(caller);
    }

    /**
     * Deferred event (ADR-0023, 2026-08-28 amendment): the dedicated receiver handler turns the
     * extracted child into a root that links to the stored context.
     */
    @Test
    void linkedProcessSpanIsARootWithALinkToTheStoredContext() {
        Map<String, String> stored =
                Map.of("traceparent", "00-11111111111111111111111111111111-2222222222222222-01");

        OutboxTracer.ProcessSpan span = outboxTracer.startProcessSpan(linkedInfo(stored));
        assertThat(registry.getCurrentObservation()).isNotNull();
        assertThat(tracer.currentSpan()).isNotNull();
        span.close();

        SimpleSpan finished = tracer.onlySpan();
        assertThat(finished.getKind()).isEqualTo(Span.Kind.CONSUMER);
        assertThat(finished.getName()).isEqualTo("outbox process T");
        assertThat(finished.context().traceId()).isNotEqualTo("11111111111111111111111111111111");
        assertThat(finished.getParentId()).isNullOrEmpty();
        assertThat(finished.getLinks()).hasSize(1);
        assertThat(finished.getLinks().get(0).getTraceContext().traceId())
                .isEqualTo("11111111111111111111111111111111");
        assertThat(finished.getLinks().get(0).getTraceContext().spanId())
                .isEqualTo("2222222222222222");
        assertThat(finished.getTags()).containsEntry("event_outboxer.propagation", "link");
        assertThat(propagator.extractedCarriers).containsExactly(stored);
    }

    @Test
    void childPropagationCarriesNoPropagationTag() {
        Map<String, String> stored =
                Map.of("traceparent", "00-11111111111111111111111111111111-2222222222222222-01");

        outboxTracer.startProcessSpan(processInfo(stored)).close();

        SimpleSpan finished = tracer.onlySpan();
        assertThat(finished.getParentId()).isEqualTo("2222222222222222");
        assertThat(finished.getLinks()).isEmpty();
        assertThat(finished.getTags()).doesNotContainKey("event_outboxer.propagation");
    }

    @Test
    void linkedProcessSpanWithUnparseableCarrierIsAnUnlinkedRoot() {
        Map<String, String> stored = Map.of("uber-trace-id", "x:y:z:1");

        outboxTracer.startProcessSpan(linkedInfo(stored)).close();

        SimpleSpan finished = tracer.onlySpan();
        assertThat(finished.getParentId()).isNullOrEmpty();
        assertThat(finished.getLinks()).isEmpty();
        assertThat(finished.getTags()).containsEntry("event_outboxer.propagation", "link");
    }

    /**
     * Without the dedicated handler the generic receiver handler claims the context: the span
     * silently stays a child. The tag still records the engine's intent, which is what makes the
     * mis-wiring visible in a trace.
     */
    @Test
    void withoutTheDedicatedHandlerALinkedEventStaysAChild() {
        TestObservationRegistry plain = TestObservationRegistry.create();
        plain.observationConfig().observationHandler(tracingHandlers(false));
        MicrometerOutboxTracer fallback = new MicrometerOutboxTracer(plain, tracer);
        Map<String, String> stored =
                Map.of("traceparent", "00-11111111111111111111111111111111-2222222222222222-01");

        fallback.startProcessSpan(linkedInfo(stored)).close();

        SimpleSpan finished = tracer.onlySpan();
        assertThat(finished.getParentId()).isEqualTo("2222222222222222");
        assertThat(finished.getLinks()).isEmpty();
        assertThat(finished.getTags()).containsEntry("event_outboxer.propagation", "link");
    }

    @Test
    void linkedTagsThePublishSpan() {
        OutboxTracer.PublishSpan span = outboxTracer.startPublishSpan(UUID.randomUUID(), "T");
        span.linked();
        span.close();

        assertThat(tracer.onlySpan().getTags()).containsEntry("event_outboxer.propagation", "link");
        TestObservationRegistryAssert.assertThat(registry)
                .hasObservationWithNameEqualTo(outboxTracer.publishObservationName())
                .that()
                .hasLowCardinalityKeyValue("event_outboxer.propagation", "link");
    }

    /**
     * {@code start()} registers a {@code LongTaskTimer} sample the registry retains until {@code
     * stop()}, and {@code SafeOutboxTracer} swallows whatever we throw — so a failure between the
     * two must not leave the observation in flight. {@code openScope()} additionally makes the
     * observation current <em>before</em> notifying the handlers, so the registry's thread-local
     * has to be released too or the next task on this pooled worker inherits it.
     */
    @Test
    void observationIsStoppedAndUnscopedWhenOpeningTheScopeFails() {
        registry.observationConfig().observationHandler(new ThrowingScopeHandler());

        assertThatThrownBy(() -> outboxTracer.startProcessSpan(processInfo(Map.of())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("scope handler exploded");

        assertThat(registry.getCurrentObservation()).isNull();
        assertThat(registry.getCurrentObservationScope()).isNull();
        TestObservationRegistryAssert.assertThat(registry)
                .hasObservationWithNameEqualTo(outboxTracer.processObservationName())
                .that()
                .hasBeenStarted()
                .hasBeenStopped();
    }

    /** Fails only when a scope is opened, leaving {@code start()} and {@code stop()} usable. */
    private static final class ThrowingScopeHandler
            implements ObservationHandler<Observation.Context> {

        @Override
        public void onScopeOpened(Observation.Context context) {
            throw new IllegalStateException("scope handler exploded");
        }

        @Override
        public boolean supportsContext(Observation.Context context) {
            return true;
        }
    }
}
