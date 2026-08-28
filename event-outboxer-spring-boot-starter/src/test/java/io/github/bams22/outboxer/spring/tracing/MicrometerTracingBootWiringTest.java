/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.OutboxTracer;
import io.github.bams22.outboxer.tracing.micrometer.MicrometerOutboxTracer;
import io.github.bams22.outboxer.tracing.micrometer.OutboxReceiverTracingObservationHandler;
import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * The Micrometer adapter delegates span creation, parent extraction and carrier injection to the
 * {@code ObservationHandler}s Spring Boot registers, and relies on Boot grouping them so that the
 * propagating handlers win over {@code DefaultTracingObservationHandler} (ADR-0023, 2026-08-16
 * amendment). Every other test builds that grouping by hand; this one boots Boot's own tracing
 * auto-configuration and asserts the assumption against the real thing.
 *
 * <p>If the grouping ever changed, the symptoms would be silent: an empty carrier in the event row
 * and span names mangled through {@code SpanNameUtil.toLowerHyphen}.
 */
class MicrometerTracingBootWiringTest {

    private static final WorkerId WORKER = new WorkerId("boot-wiring-test-worker");

    private final InMemorySpanExporter exporter = InMemorySpanExporter.create();

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner()
                    .withConfiguration(
                            AutoConfigurations.of(
                                    org.springframework.boot.actuate.autoconfigure.opentelemetry
                                            .OpenTelemetryAutoConfiguration.class,
                                    org.springframework.boot.actuate.autoconfigure.observation
                                            .ObservationAutoConfiguration.class,
                                    org.springframework.boot.actuate.autoconfigure.tracing
                                            .OpenTelemetryTracingAutoConfiguration.class,
                                    org.springframework.boot.actuate.autoconfigure.tracing
                                            .MicrometerTracingAutoConfiguration.class,
                                    MicrometerTracingAutoConfiguration.class))
                    // A SpanProcessor rather than a SpanExporter bean: Boot would wrap the latter
                    // in a BatchSpanProcessor and nothing would be exported before the assertions.
                    .withBean(
                            "testSpanProcessor",
                            SpanProcessor.class,
                            () -> SimpleSpanProcessor.create(exporter))
                    .withPropertyValues("management.tracing.sampling.probability=1.0");

    @Test
    void bootRegistersHandlersThatFillTheCarrierAndKeepTheSpanNames() {
        runner.run(
                context -> {
                    assertThat(context).getBean(OutboxTracer.class).isNotNull();
                    OutboxTracer tracer = context.getBean(OutboxTracer.class);

                    Map<String, String> stored;
                    try (OutboxTracer.PublishSpan publish =
                            tracer.startPublishSpan(UUID.randomUUID(), "ORDER_CREATED")) {
                        stored = publish.contextToStore();
                    }
                    // The propagating sender handler ran: a hand-grouped registry that let
                    // DefaultTracingObservationHandler match first would leave this empty.
                    assertThat(stored).containsKey("traceparent");

                    tracer.startProcessSpan(
                                    new OutboxTracer.ProcessSpanInfo(
                                            UUID.randomUUID(), "ORDER_CREATED", 1, WORKER, stored))
                            .close();

                    SpanData producer = span("outbox publish ORDER_CREATED");
                    SpanData consumer = span("outbox process ORDER_CREATED");
                    assertThat(consumer.getTraceId()).isEqualTo(producer.getTraceId());
                    assertThat(consumer.getParentSpanId()).isEqualTo(producer.getSpanId());
                });
    }

    /**
     * The starter's receiver handler must be asked before Boot's generic one, or a deferred event
     * (ADR-0023, 2026-08-28 amendment) silently keeps the parent-child shape. Bean order is what
     * decides the position inside Boot's first-matching tracing composite, so this checks both the
     * declared order and the resulting span shape against Boot's real registry.
     */
    @Test
    void bootAsksTheOutboxReceiverHandlerBeforeItsOwn() {
        assertThat(MicrometerTracingAutoConfiguration.RECEIVER_HANDLER_ORDER)
                .isLessThan(
                        org.springframework.boot.actuate.autoconfigure.tracing
                                .MicrometerTracingAutoConfiguration
                                .RECEIVER_TRACING_OBSERVATION_HANDLER_ORDER);
        runner.run(
                context -> {
                    assertThat(context)
                            .hasSingleBean(OutboxReceiverTracingObservationHandler.class);
                    OutboxTracer tracer = context.getBean(OutboxTracer.class);

                    Map<String, String> stored;
                    try (OutboxTracer.PublishSpan publish =
                            tracer.startPublishSpan(UUID.randomUUID(), "ORDER_CREATED")) {
                        publish.linked();
                        stored = publish.contextToStore();
                    }
                    tracer.startProcessSpan(
                                    new OutboxTracer.ProcessSpanInfo(
                                            UUID.randomUUID(),
                                            "ORDER_CREATED",
                                            1,
                                            WORKER,
                                            stored,
                                            OutboxTracer.Propagation.LINK))
                            .close();

                    SpanData producer = span("outbox publish ORDER_CREATED");
                    SpanData consumer = span("outbox process ORDER_CREATED");
                    assertThat(consumer.getParentSpanId()).isEqualTo(SpanId.getInvalid());
                    assertThat(consumer.getTraceId()).isNotEqualTo(producer.getTraceId());
                    assertThat(consumer.getLinks()).hasSize(1);
                    LinkData link = consumer.getLinks().get(0);
                    assertThat(link.getSpanContext().getTraceId()).isEqualTo(producer.getTraceId());
                    assertThat(link.getSpanContext().getSpanId()).isEqualTo(producer.getSpanId());
                    assertThat(
                                    consumer.getAttributes()
                                            .get(
                                                    io.opentelemetry.api.common.AttributeKey
                                                            .stringKey(
                                                                    "event_outboxer.propagation")))
                            .isEqualTo("link");
                    assertThat(
                                    producer.getAttributes()
                                            .get(
                                                    io.opentelemetry.api.common.AttributeKey
                                                            .stringKey(
                                                                    "event_outboxer.propagation")))
                            .isEqualTo("link");
                });
    }

    /**
     * The observations also become meters, so the starter must bind the same prefix slot the
     * metrics listener uses instead of letting the adapter fall back to its own default.
     */
    @Test
    void observationNamesFollowTheConfiguredMetricsPrefix() {
        runner.withPropertyValues("event-outboxer.metrics.prefix=myapp.outbox")
                .run(
                        context -> {
                            MicrometerOutboxTracer tracer =
                                    context.getBean(MicrometerOutboxTracer.class);
                            assertThat(tracer.publishObservationName())
                                    .isEqualTo("myapp.outbox.publish");
                            assertThat(tracer.processObservationName())
                                    .isEqualTo("myapp.outbox.process");
                        });
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
