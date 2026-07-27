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
import static org.awaitility.Awaitility.await;

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.api.handle.FailureDecision;
import io.github.bams22.outboxer.api.handle.FailureHandler;
import io.github.bams22.outboxer.api.publish.OutboxEventPublisher;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spring.storage.OutboxInMemoryTestConfiguration;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

/**
 * End-to-end trace continuity (ADR-0023) through the real starter wiring: a caller span around
 * {@code publish()} is continued by the PRODUCER span, whose stored context parents the CONSUMER
 * span around the handler — one trace across the outbox hop, including the retry attempt after a
 * handler failure.
 */
@SpringBootTest(
    classes = TracingEndToEndTest.TestApp.class,
    properties = {
      "event-outboxer.publisher.no-transaction-policy=IGNORE",
      "event-outboxer.event-types.defaults.poll-min-interval=20ms",
      "event-outboxer.event-types.defaults.poll-max-interval=50ms",
      "event-outboxer.event-types.defaults.handler-pool-size=2",
      "event-outboxer.maintenance.heartbeat-interval=200ms",
      "event-outboxer.maintenance.dead-threshold=1s",
      "event-outboxer.maintenance.orphan-recovery-interval=500ms",
      "event-outboxer.maintenance.watchdog-interval=500ms"
    })
@Import(OutboxInMemoryTestConfiguration.class)
class TracingEndToEndTest {

  @Autowired OutboxEventPublisher publisher;
  @Autowired OpenTelemetry openTelemetry;
  @Autowired InMemorySpanExporter exporter;
  @Autowired FailOnceHandler handler;

  @Test
  void traceContinuesFromCallerThroughProducerToConsumerSpansAcrossRetries() {
    Span caller = openTelemetry.getTracer("test-caller").spanBuilder("business-op").startSpan();
    UUID id;
    try (Scope scope = caller.makeCurrent()) {
      id = publisher.publish("TRACED", new Payload("p-1"));
    }
    caller.end();

    // First attempt throws, the zero-delay failure handler re-pends, second attempt succeeds.
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(20))
        .until(() -> handler.attempts.get() >= 2);
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(20))
        .until(
            () ->
                exporter.getFinishedSpanItems().stream()
                        .filter(s -> s.getName().equals("outbox process TRACED"))
                        .count()
                    >= 2);

    List<SpanData> spans = exporter.getFinishedSpanItems();
    SpanData business =
        spans.stream().filter(s -> s.getName().equals("business-op")).findFirst().orElseThrow();
    SpanData producer =
        spans.stream()
            .filter(s -> s.getName().equals("outbox publish TRACED"))
            .findFirst()
            .orElseThrow();
    List<SpanData> consumers =
        spans.stream().filter(s -> s.getName().equals("outbox process TRACED")).toList();

    // One trace across the whole hop.
    assertThat(producer.getKind()).isEqualTo(SpanKind.PRODUCER);
    assertThat(producer.getTraceId()).isEqualTo(business.getTraceId());
    assertThat(producer.getParentSpanId()).isEqualTo(business.getSpanId());
    assertThat(producer.getAttributes().get(AttributeKey.stringKey("messaging.message.id")))
        .isEqualTo(id.toString());

    assertThat(consumers).hasSize(2);
    for (SpanData consumer : consumers) {
      assertThat(consumer.getKind()).isEqualTo(SpanKind.CONSUMER);
      assertThat(consumer.getTraceId()).isEqualTo(business.getTraceId());
      assertThat(consumer.getParentSpanId()).isEqualTo(producer.getSpanId());
      assertThat(consumer.getAttributes().get(AttributeKey.stringKey("event_outboxer.worker.id")))
          .isNotBlank();
    }

    SpanData firstAttempt =
        consumers.stream()
            .filter(
                s -> Long.valueOf(1L).equals(s.getAttributes().get(AttributeKey.longKey("event_outboxer.attempt"))))
            .findFirst()
            .orElseThrow();
    SpanData secondAttempt =
        consumers.stream()
            .filter(
                s -> Long.valueOf(2L).equals(s.getAttributes().get(AttributeKey.longKey("event_outboxer.attempt"))))
            .findFirst()
            .orElseThrow();

    assertThat(firstAttempt.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
    assertThat(firstAttempt.getEvents())
        .anySatisfy(event -> assertThat(event.getName()).isEqualTo("exception"));
    assertThat(secondAttempt.getStatus().getStatusCode()).isNotEqualTo(StatusCode.ERROR);
  }

  record Payload(String value) {}

  static class FailOnceHandler implements EventHandler<Payload> {
    final AtomicInteger attempts = new AtomicInteger();

    @Override
    public String eventType() {
      return "TRACED";
    }

    @Override
    public Class<Payload> payloadType() {
      return Payload.class;
    }

    @Override
    public EventOutcome handle(EventContext ctx, Payload payload) {
      if (attempts.incrementAndGet() == 1) {
        throw new IllegalStateException("first attempt fails");
      }
      return EventOutcome.Success.INSTANCE;
    }
  }

  @SpringBootConfiguration
  @EnableAutoConfiguration(
      exclude = {
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        JdbcTemplateAutoConfiguration.class
      })
  static class TestApp {

    @Bean
    FailOnceHandler failOnceHandler() {
      return new FailOnceHandler();
    }

    @Bean
    InMemorySpanExporter inMemorySpanExporter() {
      return InMemorySpanExporter.create();
    }

    @Bean
    OpenTelemetry openTelemetry(InMemorySpanExporter exporter) {
      return OpenTelemetrySdk.builder()
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
    }

    /**
     * Zero-delay retry instead of the default 5-second exponential backoff, so the second
     * attempt lands within the test's await window.
     */
    @Bean("outboxDefaultFailureHandler")
    FailureHandler<Object> outboxDefaultFailureHandler(Clock clock) {
      return ctx -> new FailureDecision.RetryAt(clock.now(), "test: retry immediately");
    }
  }
}
