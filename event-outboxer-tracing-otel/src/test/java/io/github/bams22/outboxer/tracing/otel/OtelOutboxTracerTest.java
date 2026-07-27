/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.tracing.otel;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.OutboxTracer;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OtelOutboxTracerTest {

  private static final WorkerId WORKER = new WorkerId("otel-test-worker");

  private InMemorySpanExporter exporter;
  private OpenTelemetrySdk sdk;
  private OtelOutboxTracer tracer;

  @BeforeEach
  void setUp() {
    exporter = InMemorySpanExporter.create();
    sdk =
        OpenTelemetrySdk.builder()
            .setTracerProvider(
                SdkTracerProvider.builder()
                    .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                    .build())
            .setPropagators(
                ContextPropagators.create(
                    io.opentelemetry.context.propagation.TextMapPropagator.composite(
                        io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
                            .getInstance(),
                        W3CBaggagePropagator.getInstance())))
            .build();
    tracer = new OtelOutboxTracer(sdk);
  }

  @AfterEach
  void tearDown() {
    sdk.close();
  }

  private static OutboxTracer.ProcessSpanInfo processInfo(
      UUID eventId, Map<String, String> stored) {
    return new OutboxTracer.ProcessSpanInfo(eventId, "T", 1, WORKER, stored);
  }

  @Test
  void publishSpanCapturesItsOwnContext() {
    UUID id = UUID.randomUUID();

    OutboxTracer.PublishSpan span = tracer.startPublishSpan(id, "T");
    Map<String, String> stored = span.contextToStore();
    span.close();

    assertThat(stored).containsKey("traceparent");
    List<SpanData> finished = exporter.getFinishedSpanItems();
    assertThat(finished).hasSize(1);
    SpanData data = finished.get(0);
    assertThat(data.getName()).isEqualTo("outbox publish T");
    assertThat(data.getKind()).isEqualTo(SpanKind.PRODUCER);
    assertThat(stored.get("traceparent")).contains(data.getTraceId()).contains(data.getSpanId());
    assertThat(data.getAttributes().get(AttributeKey.stringKey("messaging.system")))
        .isEqualTo("event_outboxer");
    assertThat(data.getAttributes().get(AttributeKey.stringKey("messaging.operation.type")))
        .isEqualTo("send");
    assertThat(data.getAttributes().get(AttributeKey.stringKey("messaging.destination.name")))
        .isEqualTo("T");
    assertThat(data.getAttributes().get(AttributeKey.stringKey("messaging.message.id")))
        .isEqualTo(id.toString());
    assertThat(data.getInstrumentationScopeInfo().getName())
        .isEqualTo(OtelOutboxTracer.INSTRUMENTATION_SCOPE);
  }

  @Test
  void publishSpanIsChildOfTheCallerSpanAndCarriesBaggage() {
    Tracer callerTracer = sdk.getTracer("test-caller");
    Span caller = callerTracer.spanBuilder("business-op").startSpan();
    Map<String, String> stored;
    try (Scope callerScope = caller.makeCurrent();
        Scope baggageScope =
            Baggage.builder().put("tenant", "acme").build().makeCurrent()) {
      OutboxTracer.PublishSpan span = tracer.startPublishSpan(UUID.randomUUID(), "T");
      stored = span.contextToStore();
      span.close();
    }
    caller.end();

    List<SpanData> finished = exporter.getFinishedSpanItems();
    SpanData producer =
        finished.stream().filter(s -> s.getName().equals("outbox publish T")).findFirst()
            .orElseThrow();
    SpanData business =
        finished.stream().filter(s -> s.getName().equals("business-op")).findFirst().orElseThrow();
    assertThat(producer.getTraceId()).isEqualTo(business.getTraceId());
    assertThat(producer.getParentSpanId()).isEqualTo(business.getSpanId());
    // Baggage is injected as a flat single-string header value — FlatMapJson-compatible.
    assertThat(stored.get("baggage")).contains("tenant=acme");
    assertThat(stored.values()).allSatisfy(v -> assertThat(v).isInstanceOf(String.class));
  }

  @Test
  void processSpanContinuesTheStoredTraceAsChildOfProducer() {
    UUID id = UUID.randomUUID();
    OutboxTracer.PublishSpan publish = tracer.startPublishSpan(id, "T");
    Map<String, String> stored = publish.contextToStore();
    publish.close();

    OutboxTracer.ProcessSpan process = tracer.startProcessSpan(processInfo(id, stored));
    process.close();

    List<SpanData> finished = exporter.getFinishedSpanItems();
    SpanData producer =
        finished.stream().filter(s -> s.getKind() == SpanKind.PRODUCER).findFirst().orElseThrow();
    SpanData consumer =
        finished.stream().filter(s -> s.getKind() == SpanKind.CONSUMER).findFirst().orElseThrow();
    assertThat(consumer.getName()).isEqualTo("outbox process T");
    assertThat(consumer.getTraceId()).isEqualTo(producer.getTraceId());
    assertThat(consumer.getParentSpanId()).isEqualTo(producer.getSpanId());
    assertThat(consumer.getAttributes().get(AttributeKey.longKey("event_outboxer.attempt")))
        .isEqualTo(1L);
    assertThat(consumer.getAttributes().get(AttributeKey.stringKey("event_outboxer.worker.id")))
        .isEqualTo(WORKER.value());
    assertThat(consumer.getAttributes().get(AttributeKey.stringKey("messaging.operation.type")))
        .isEqualTo("process");
  }

  @Test
  void processSpanIsCurrentAndBaggageRestoredWhileOpen() {
    Map<String, String> stored;
    try (Scope baggageScope = Baggage.builder().put("tenant", "acme").build().makeCurrent()) {
      OutboxTracer.PublishSpan publish = tracer.startPublishSpan(UUID.randomUUID(), "T");
      stored = publish.contextToStore();
      publish.close();
    }

    Span before = Span.current();
    OutboxTracer.ProcessSpan process = tracer.startProcessSpan(processInfo(UUID.randomUUID(), stored));
    Span during = Span.current();
    String tenantDuring = Baggage.current().getEntryValue("tenant");
    process.close();
    Span after = Span.current();

    assertThat(during.getSpanContext().isValid()).isTrue();
    assertThat(during).isNotEqualTo(before);
    assertThat(tenantDuring).isEqualTo("acme"); // extracted baggage restored for the handler
    assertThat(after).isEqualTo(before); // scope fully unwound
  }

  @Test
  void errorRecordsExceptionAndErrorStatus() {
    OutboxTracer.ProcessSpan process =
        tracer.startProcessSpan(processInfo(UUID.randomUUID(), Map.of()));
    IllegalStateException boom = new IllegalStateException("handler exploded");
    process.error(boom);
    process.close();

    SpanData data = exporter.getFinishedSpanItems().get(0);
    assertThat(data.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
    assertThat(data.getEvents())
        .anySatisfy(event -> assertThat(event.getName()).isEqualTo("exception"));
  }

  @Test
  void emptyStoredContextStartsANewTrace() {
    OutboxTracer.ProcessSpan process =
        tracer.startProcessSpan(processInfo(UUID.randomUUID(), Map.of()));
    process.close();

    SpanData data = exporter.getFinishedSpanItems().get(0);
    assertThat(data.getParentSpanId()).isEqualTo("0000000000000000"); // root span
    assertThat(data.getKind()).isEqualTo(SpanKind.CONSUMER);
  }

  @Test
  void coalescedTagsTheProducerSpan() {
    UUID existing = UUID.randomUUID();
    OutboxTracer.PublishSpan span = tracer.startPublishSpan(UUID.randomUUID(), "T");
    span.coalesced(existing);
    span.close();

    SpanData data = exporter.getFinishedSpanItems().get(0);
    assertThat(
            data.getAttributes().get(AttributeKey.stringKey("event_outboxer.coalesced_into")))
        .isEqualTo(existing.toString());
  }

  @Test
  void closeIsIdempotent() {
    OutboxTracer.PublishSpan publish = tracer.startPublishSpan(UUID.randomUUID(), "T");
    publish.close();
    publish.close();
    OutboxTracer.ProcessSpan process =
        tracer.startProcessSpan(processInfo(UUID.randomUUID(), Map.of()));
    process.close();
    process.close();

    assertThat(exporter.getFinishedSpanItems()).hasSize(2);
  }

  @Test
  void noopOpenTelemetryDegradesToEmptyContext() {
    OtelOutboxTracer noop = new OtelOutboxTracer(OpenTelemetry.noop());

    OutboxTracer.PublishSpan publish = noop.startPublishSpan(UUID.randomUUID(), "T");
    assertThat(publish.contextToStore()).isEmpty();
    publish.close();

    OutboxTracer.ProcessSpan process =
        noop.startProcessSpan(processInfo(UUID.randomUUID(), Map.of()));
    process.close(); // nothing throws
  }
}
