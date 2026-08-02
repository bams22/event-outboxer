/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PendingEventTest {

  private static PendingEvent.PendingEventBuilder validBuilder() {
    return PendingEvent.builder()
        .id(UUID.randomUUID())
        .eventType("TEST_EVENT")
        .payload(SerializedPayload.ofText("{}"))
        .payloadFormat("test-json")
        .payloadClass("com.example.TestPayload")
        .priority((short) 0)
        .runAt(Instant.now())
        .traceContext(Map.of());
  }

  @Test
  void builderProducesExpectedRecord() {
    UUID id = UUID.randomUUID();
    Instant now = Instant.parse("2026-01-01T00:00:00Z");
    PendingEvent e =
        PendingEvent.builder()
            .id(id)
            .eventType("T")
            .payload(SerializedPayload.ofText("{\"x\":1}"))
            .payloadFormat("test-json")
            .payloadClass("com.example.X")
            .priority((short) 3)
            .runAt(now)
            .traceContext(Map.of("traceparent", "00-x-y-z"))
            .build();
    assertThat(e.id()).isEqualTo(id);
    assertThat(e.priority()).isEqualTo((short) 3);
    assertThat(e.payloadFormat()).isEqualTo("test-json");
    assertThat(e.traceContext()).containsEntry("traceparent", "00-x-y-z");
  }

  @Test
  void rejectsBlankPayloadFormat() {
    assertThatThrownBy(() -> validBuilder().payloadFormat(" ").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("payloadFormat");
  }

  @Test
  void rejectsOversizedPayloadFormat() {
    assertThatThrownBy(() -> validBuilder().payloadFormat("x".repeat(65)).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("payloadFormat");
  }

  @Test
  void rejectsBlankEventType() {
    assertThatThrownBy(() -> validBuilder().eventType("").build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsNullPayload() {
    assertThatThrownBy(() -> validBuilder().payload(null).build())
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void rejectsNullTraceContext() {
    assertThatThrownBy(() -> validBuilder().traceContext(null).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("traceContext");
  }

  @Test
  void traceContextIsCopiedIntoAnImmutableMap() {
    PendingEvent e = validBuilder().build();
    assertThat(e.traceContext()).isEmpty();
    assertThatThrownBy(() -> e.traceContext().put("x", "y"))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
