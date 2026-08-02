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

import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.OutboxTracer;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.propagation.Propagator;
import io.micrometer.tracing.test.simple.SimpleSpan;
import io.micrometer.tracing.test.simple.SimpleTracer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class MicrometerOutboxTracerTest {

    private static final WorkerId WORKER = new WorkerId("micrometer-test-worker");

    private final SimpleTracer tracer = new SimpleTracer();
    private final RecordingPropagator propagator = new RecordingPropagator(tracer);
    private final MicrometerOutboxTracer outboxTracer =
            new MicrometerOutboxTracer(tracer, propagator);

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
    void processSpanExtractsStoredCarrierAndBecomesCurrent() {
        Map<String, String> stored =
                Map.of("traceparent", "00-11111111111111111111111111111111-2222222222222222-01");

        OutboxTracer.ProcessSpan span = outboxTracer.startProcessSpan(processInfo(stored));
        Span current = tracer.currentSpan();
        span.close();

        assertThat(propagator.extractedCarriers).containsExactly(stored);
        SimpleSpan finished = tracer.onlySpan();
        assertThat(current).isNotNull();
        assertThat(tracer.currentSpan()).isNull(); // scope closed with the handle
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
}
