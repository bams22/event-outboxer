/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.serializer.jackson;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.exception.PublishSerializationException;
import io.github.bams22.outboxer.spi.EventSerializer;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JacksonEventSerializerTest {

  private final EventSerializer serializer =
      new JacksonEventSerializer(JacksonObjectMapperFactory.defaults());

  @Test
  void declaresStableJacksonJsonFormat() {
    assertThat(serializer.format()).isEqualTo("jackson-json");
    assertThat(JacksonEventSerializer.FORMAT).isEqualTo("jackson-json");
  }

  @Test
  void serializesIntoTheTextLane() {
    SerializedPayload payload =
        serializer.serialize(new OrderCreated("ord-1", 1, Instant.parse("2026-04-21T12:00:00Z")));

    assertThat(payload.isText()).isTrue();
  }

  @Test
  void roundTripsSimpleRecord() {
    OrderCreated original = new OrderCreated("ord-1", 42, Instant.parse("2026-04-21T12:00:00Z"));

    SerializedPayload payload = serializer.serialize(original);
    OrderCreated parsed = serializer.deserialize(payload, OrderCreated.class);

    assertThat(parsed).isEqualTo(original);
  }

  @Test
  void writesInstantsAsIsoStringsNotEpochMillis() {
    OrderCreated original = new OrderCreated("ord-1", 1, Instant.parse("2026-04-21T12:00:00Z"));

    String json = serializer.serialize(original).requireText();

    assertThat(json).contains("2026-04-21T12:00:00Z");
    assertThat(json).doesNotContain("1745");
  }

  @Test
  void roundTripsRecordsWithCollections() {
    Email email =
        new Email(
            "user@example.com",
            List.of("inbox", "important"),
            Map.of("priority", "normal", "source", "api"));

    SerializedPayload payload = serializer.serialize(email);
    Email parsed = serializer.deserialize(payload, Email.class);

    assertThat(parsed).isEqualTo(email);
  }

  @Test
  void roundTripsPolymorphicDto() {
    Shape shape = new Circle(3.0);

    SerializedPayload payload = serializer.serialize(shape);
    Shape parsed = serializer.deserialize(payload, Shape.class);

    assertThat(parsed).isEqualTo(shape);
    assertThat(parsed).isInstanceOf(Circle.class);
  }

  @Test
  void ignoresUnknownPropertiesByDefault() {
    // ADR-0011: evolution-friendly default — a payload written by a NEWER DTO version (extra
    // field) must deserialize on an older replica during a rolling deploy.
    String json =
        "{\"orderId\":\"ord-1\",\"count\":1,\"occurredAt\":\"2026-04-21T12:00:00Z\",\"ghostField\":true}";

    OrderCreated parsed =
        serializer.deserialize(SerializedPayload.ofText(json), OrderCreated.class);

    assertThat(parsed)
        .isEqualTo(new OrderCreated("ord-1", 1, Instant.parse("2026-04-21T12:00:00Z")));
  }

  @Test
  void toleratesMissingFieldWithPrimitiveDefault() {
    // A payload written by an OLDER DTO version (field not yet present) must deserialize on a
    // newer replica: absent primitive → default value.
    String json = "{\"orderId\":\"ord-1\",\"occurredAt\":\"2026-04-21T12:00:00Z\"}";

    OrderCreated parsed =
        serializer.deserialize(SerializedPayload.ofText(json), OrderCreated.class);

    assertThat(parsed.count()).isZero();
  }

  @Test
  void toleratesExplicitNullForPrimitive() {
    // FAIL_ON_NULL_FOR_PRIMITIVES is deliberately off: for record DTOs Jackson routes an ABSENT
    // primitive component through the same null path as an explicit null, so enabling the
    // feature would break the add-a-primitive-field evolution case. Both resolve to the default.
    String json = "{\"orderId\":\"ord-1\",\"count\":null,\"occurredAt\":\"2026-04-21T12:00:00Z\"}";

    OrderCreated parsed =
        serializer.deserialize(SerializedPayload.ofText(json), OrderCreated.class);

    assertThat(parsed.count()).isZero();
  }

  @Test
  void raisesPublishExceptionOnUnserializableInput() {
    ObjectMapper mapper = JacksonObjectMapperFactory.defaults();
    EventSerializer noSelfRef = new JacksonEventSerializer(mapper);
    SelfRef bad = new SelfRef();
    bad.self = bad;

    assertThatThrownBy(() -> noSelfRef.serialize(bad))
        .isInstanceOf(PublishSerializationException.class);
  }

  @Test
  void rejectsBinaryLaneInput() {
    assertThatThrownBy(
            () ->
                serializer.deserialize(
                    SerializedPayload.ofBytes(new byte[] {0x00, (byte) 0xFF}), OrderCreated.class))
        .isInstanceOf(IllegalStateException.class);
  }

  // --- fixtures ---

  record OrderCreated(String orderId, int count, Instant occurredAt) {}

  record Email(String to, List<String> tags, Map<String, String> headers) {}

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "shape")
  @JsonSubTypes({
    @JsonSubTypes.Type(value = Circle.class, name = "circle"),
    @JsonSubTypes.Type(value = Square.class, name = "square")
  })
  sealed interface Shape permits Circle, Square {}

  record Circle(double radius) implements Shape {}

  record Square(double side) implements Shape {}

  static final class SelfRef {
    public SelfRef self;
  }
}
