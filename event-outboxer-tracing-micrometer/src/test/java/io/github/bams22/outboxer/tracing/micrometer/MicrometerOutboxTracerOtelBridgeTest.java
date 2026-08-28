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
import io.micrometer.context.ContextSnapshot;
import io.micrometer.context.ContextSnapshotFactory;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingReceiverTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingSenderTracingObservationHandler;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Behaviour of the adapter against the real {@code micrometer-tracing-bridge-otel} and OTel SDK —
 * the properties the Observation-based wiring exists for (ADR-0023, 2026-08-16 amendment) and that
 * a {@code SimpleTracer} double cannot show: handler-side nesting, context propagation across a
 * thread hop inside the handler, and parent isolation when the stored carrier is empty.
 */
class MicrometerOutboxTracerOtelBridgeTest {

    private static final WorkerId WORKER = new WorkerId("otel-bridge-test-worker");

    private final InMemorySpanExporter exporter = InMemorySpanExporter.create();
    private final OpenTelemetrySdk sdk =
            OpenTelemetrySdk.builder()
                    .setTracerProvider(
                            SdkTracerProvider.builder()
                                    .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                                    .build())
                    .setPropagators(
                            ContextPropagators.create(
                                    TextMapPropagator.composite(
                                            W3CTraceContextPropagator.getInstance(),
                                            W3CBaggagePropagator.getInstance())))
                    .build();
    private final io.opentelemetry.api.trace.Tracer otelTracer = sdk.getTracer("test");
    private final OtelTracer tracer =
            new OtelTracer(otelTracer, new OtelCurrentTraceContext(), event -> {});
    private final ObservationRegistry registry = ObservationRegistry.create();
    private final ExecutorService pool = Executors.newSingleThreadExecutor();
    private final MicrometerOutboxTracer outboxTracer =
            new MicrometerOutboxTracer(registry, tracer);

    /**
     * The handler set Spring Boot registers for a Micrometer Tracing application — grouped exactly
     * as Boot's {@code ObservationHandlerGrouping} does it, so that a context is handled by the
     * first matching tracing handler only.
     */
    @BeforeEach
    void registerTracingHandlers() {
        OtelPropagator propagator = new OtelPropagator(sdk.getPropagators(), otelTracer);
        registry.observationConfig()
                .observationHandler(
                        new ObservationHandler.FirstMatchingCompositeObservationHandler(
                                new OutboxReceiverTracingObservationHandler(tracer, propagator),
                                new PropagatingReceiverTracingObservationHandler<>(
                                        tracer, propagator),
                                new PropagatingSenderTracingObservationHandler<>(
                                        tracer, propagator),
                                new DefaultTracingObservationHandler(tracer)));
    }

    @AfterEach
    void shutdown() {
        pool.shutdownNow();
        // Each test builds its own SdkTracerProvider (which registers a JVM shutdown hook) and
        // exporter; without this they accumulate for the lifetime of the surefire JVM.
        sdk.close();
    }

    @Test
    void handlerObservationsNestUnderTheConsumerSpan() {
        Map<String, String> stored = publishAndCaptureContext();

        try (OutboxTracer.ProcessSpan ignored =
                outboxTracer.startProcessSpan(processInfo(stored))) {
            Observation.createNotStarted("jdbc.query", registry).observe(() -> {});
        }

        SpanData consumer = span("outbox process T");
        SpanData jdbc = span("jdbc.query");
        assertThat(consumer.getKind()).isEqualTo(SpanKind.CONSUMER);
        assertThat(consumer.getParentSpanId()).isEqualTo(span("outbox publish T").getSpanId());
        assertThat(jdbc.getParentSpanId()).isEqualTo(consumer.getSpanId());
        assertThat(jdbc.getTraceId()).isEqualTo(consumer.getTraceId());
    }

    @Test
    void contextSurvivesAThreadHopInsideTheHandler() throws Exception {
        Map<String, String> stored = publishAndCaptureContext();

        try (OutboxTracer.ProcessSpan ignored =
                outboxTracer.startProcessSpan(processInfo(stored))) {
            // What ContextPropagatingTaskDecorator does when handler code offloads work.
            ContextSnapshot snapshot = ContextSnapshotFactory.builder().build().captureAll();
            pool.submit(
                            () -> {
                                try (ContextSnapshot.Scope restored = snapshot.setThreadLocals()) {
                                    Observation.createNotStarted("async.work", registry)
                                            .observe(() -> {});
                                }
                            })
                    .get();
        }

        SpanData consumer = span("outbox process T");
        SpanData async = span("async.work");
        assertThat(async.getTraceId()).isEqualTo(consumer.getTraceId());
        assertThat(async.getParentSpanId()).isEqualTo(consumer.getSpanId());
    }

    @Test
    void emptyStoredContextDoesNotAdoptAnAmbientSpan() {
        Span ambient = otelTracer.spanBuilder("ambient").startSpan();
        try (Scope ignored = ambient.makeCurrent()) {
            outboxTracer.startProcessSpan(processInfo(Map.of())).close();
        }
        ambient.end();

        SpanData consumer = span("outbox process T");
        assertThat(consumer.getParentSpanId()).isEqualTo(SpanId.getInvalid());
        assertThat(consumer.getTraceId()).isNotEqualTo(span("ambient").getTraceId());
    }

    @Test
    void publishSpanLeavesTheCallerThreadUntouched() {
        Span caller = otelTracer.spanBuilder("business-op").startSpan();
        try (Scope ignored = caller.makeCurrent();
                OutboxTracer.PublishSpan publish =
                        outboxTracer.startPublishSpan(UUID.randomUUID(), "T")) {
            assertThat(Span.current().getSpanContext().getSpanId())
                    .isEqualTo(caller.getSpanContext().getSpanId());
            assertThat(registry.getCurrentObservation()).isNull();
            Observation.createNotStarted("jdbc.insert", registry).observe(() -> {});
        }
        caller.end();

        SpanData producer = span("outbox publish T");
        assertThat(producer.getKind()).isEqualTo(SpanKind.PRODUCER);
        assertThat(producer.getParentSpanId()).isEqualTo(caller.getSpanContext().getSpanId());
        // The insert belongs to the caller's span, not to the producer span (SPI contract).
        assertThat(span("jdbc.insert").getParentSpanId())
                .isEqualTo(caller.getSpanContext().getSpanId());
    }

    /**
     * Deferred event (ADR-0023, 2026-08-28 amendment) on the real OTel bridge: {@code
     * setNoParent()} after the propagator's {@code setParent()} yields a root span that still
     * carries the extracted context — so the link, the baggage, the current observation and
     * handler-side nesting all hold at once.
     */
    @Test
    void deferredEventGetsARootConsumerSpanLinkedToTheProducer() {
        Map<String, String> stored;
        Span caller = otelTracer.spanBuilder("business-op").startSpan();
        try (Scope ignored = caller.makeCurrent();
                Scope baggage = Baggage.builder().put("tenant", "acme").build().makeCurrent();
                OutboxTracer.PublishSpan publish =
                        outboxTracer.startPublishSpan(UUID.randomUUID(), "T")) {
            stored = publish.contextToStore();
        }
        caller.end();
        assertThat(stored).containsKey("baggage");

        String tenantDuring;
        try (OutboxTracer.ProcessSpan ignored =
                outboxTracer.startProcessSpan(
                        new OutboxTracer.ProcessSpanInfo(
                                UUID.randomUUID(),
                                "T",
                                1,
                                WORKER,
                                stored,
                                OutboxTracer.Propagation.LINK))) {
            tenantDuring = Baggage.current().getEntryValue("tenant");
            assertThat(registry.getCurrentObservation()).isNotNull();
            Observation.createNotStarted("jdbc.query", registry).observe(() -> {});
        }

        SpanData producer = span("outbox publish T");
        SpanData consumer = span("outbox process T");
        SpanData jdbc = span("jdbc.query");
        assertThat(consumer.getParentSpanId()).isEqualTo(SpanId.getInvalid());
        assertThat(consumer.getTraceId()).isNotEqualTo(producer.getTraceId());
        assertThat(consumer.getLinks()).hasSize(1);
        LinkData link = consumer.getLinks().get(0);
        assertThat(link.getSpanContext().getTraceId()).isEqualTo(producer.getTraceId());
        assertThat(link.getSpanContext().getSpanId()).isEqualTo(producer.getSpanId());
        assertThat(
                        consumer.getAttributes()
                                .get(AttributeKey.stringKey("event_outboxer.propagation")))
                .isEqualTo("link");
        assertThat(tenantDuring).isEqualTo("acme");
        assertThat(jdbc.getTraceId()).isEqualTo(consumer.getTraceId());
        assertThat(jdbc.getParentSpanId()).isEqualTo(consumer.getSpanId());
    }

    private Map<String, String> publishAndCaptureContext() {
        Span caller = otelTracer.spanBuilder("business-op").startSpan();
        Map<String, String> stored;
        try (Scope ignored = caller.makeCurrent();
                OutboxTracer.PublishSpan publish =
                        outboxTracer.startPublishSpan(UUID.randomUUID(), "T")) {
            stored = publish.contextToStore();
        }
        caller.end();
        assertThat(stored).containsKey("traceparent");
        return stored;
    }

    private OutboxTracer.ProcessSpanInfo processInfo(Map<String, String> stored) {
        return new OutboxTracer.ProcessSpanInfo(UUID.randomUUID(), "T", 1, WORKER, stored);
    }

    private SpanData span(String name) {
        List<SpanData> matches =
                exporter.getFinishedSpanItems().stream()
                        .filter(s -> s.getName().equals(name))
                        .toList();
        assertThat(matches).as("finished span named '%s'", name).hasSize(1);
        return matches.getFirst();
    }
}
