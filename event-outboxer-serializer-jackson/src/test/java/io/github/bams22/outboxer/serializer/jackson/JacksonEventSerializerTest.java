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
import io.github.bams22.outboxer.domain.exception.PayloadDeserializationException;
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
  void roundTripsSimpleRecord() {
    OrderCreated original = new OrderCreated("ord-1", 42, Instant.parse("2026-04-21T12:00:00Z"));

    String json = serializer.serialize(original);
    OrderCreated parsed = serializer.deserialize(json, OrderCreated.class);

    assertThat(parsed).isEqualTo(original);
  }

  @Test
  void writesInstantsAsIsoStringsNotEpochMillis() {
    OrderCreated original = new OrderCreated("ord-1", 1, Instant.parse("2026-04-21T12:00:00Z"));

    String json = serializer.serialize(original);

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

    String json = serializer.serialize(email);
    Email parsed = serializer.deserialize(json, Email.class);

    assertThat(parsed).isEqualTo(email);
  }

  @Test
  void roundTripsPolymorphicDto() {
    Shape shape = new Circle(3.0);

    String json = serializer.serialize(shape);
    Shape parsed = serializer.deserialize(json, Shape.class);

    assertThat(parsed).isEqualTo(shape);
    assertThat(parsed).isInstanceOf(Circle.class);
  }

  @Test
  void failsOnUnknownPropertiesByDefault() {
    String json = "{\"orderId\":\"ord-1\",\"count\":1,\"occurredAt\":\"2026-04-21T12:00:00Z\",\"ghostField\":true}";

    assertThatThrownBy(() -> serializer.deserialize(json, OrderCreated.class))
        .isInstanceOf(PayloadDeserializationException.class);
  }

  @Test
  void raisesPublishExceptionOnUnserializableInput() {
    ObjectMapper mapper = JacksonObjectMapperFactory.defaults();
    EventSerializer noSelfRef = new JacksonEventSerializer(mapper);
    SelfRef bad = new SelfRef();
    bad.self = bad;

    assertThatThrownBy(() -> noSelfRef.serialize(bad)).isInstanceOf(PublishSerializationException.class);
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
