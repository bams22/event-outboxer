/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.serializer.protobuf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.exception.PayloadDeserializationException;
import io.github.bams22.outboxer.domain.exception.PublishSerializationException;
import io.github.bams22.outboxer.serializer.protobuf.testproto.EmptyEventProto;
import io.github.bams22.outboxer.serializer.protobuf.testproto.OrderCreatedProto;
import io.github.bams22.outboxer.serializer.protobuf.testproto.OrderStatusProto;
import io.github.bams22.outboxer.spi.EventSerializer;
import org.junit.jupiter.api.Test;

class ProtobufEventSerializerTest {

    private final EventSerializer serializer = new ProtobufEventSerializer();

    @Test
    void declaresStableProtobufFormat() {
        assertThat(serializer.format()).isEqualTo("protobuf");
        assertThat(ProtobufEventSerializer.FORMAT).isEqualTo("protobuf");
    }

    @Test
    void serializesIntoTheBinaryLane() {
        SerializedPayload payload = serializer.serialize(simpleOrder());

        assertThat(payload.isText()).isFalse();
        assertThat(payload.requireBytes()).isNotEmpty();
        assertThatThrownBy(payload::requireText).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void roundTripsSimpleMessage() {
        OrderCreatedProto original = simpleOrder();

        SerializedPayload payload = serializer.serialize(original);
        OrderCreatedProto parsed = serializer.deserialize(payload, OrderCreatedProto.class);

        assertThat(parsed).isEqualTo(original);
    }

    @Test
    void roundTripsRepeatedMapNestedAndEnumFields() {
        OrderCreatedProto original =
                OrderCreatedProto.newBuilder()
                        .setOrderId("ord-2")
                        .setCount(3)
                        .addTags("vip")
                        .addTags("gift")
                        .putAttributes("priority", "high")
                        .putAttributes("source", "api")
                        .addLines(
                                OrderCreatedProto.OrderLineProto.newBuilder()
                                        .setSku("sku-1")
                                        .setQuantity(2))
                        .addLines(
                                OrderCreatedProto.OrderLineProto.newBuilder()
                                        .setSku("sku-2")
                                        .setQuantity(5))
                        .setStatus(OrderStatusProto.ORDER_STATUS_PAID)
                        .build();

        SerializedPayload payload = serializer.serialize(original);
        OrderCreatedProto parsed = serializer.deserialize(payload, OrderCreatedProto.class);

        assertThat(parsed).isEqualTo(original);
    }

    @Test
    void byteExactRoundTrip() {
        // Map-free message on purpose: map serialization order is not guaranteed stable, so only
        // map-free payloads are safe to compare at the byte level after a re-serialize.
        OrderCreatedProto original = simpleOrder();
        byte[] wire = original.toByteArray();

        SerializedPayload payload = serializer.serialize(original);
        assertThat(payload.requireBytes()).isEqualTo(wire);

        OrderCreatedProto parsed =
                serializer.deserialize(SerializedPayload.ofBytes(wire), OrderCreatedProto.class);
        assertThat(serializer.serialize(parsed).requireBytes()).isEqualTo(wire);
    }

    @Test
    void roundTripsEmptyMessageAsZeroBytes() {
        // Zero bytes IS a valid protobuf message; SerializedPayload.ofBytes(new byte[0]) is legal.
        SerializedPayload payload = serializer.serialize(EmptyEventProto.getDefaultInstance());

        assertThat(payload.requireBytes()).isEmpty();
        assertThat(serializer.deserialize(payload, EmptyEventProto.class))
                .isEqualTo(EmptyEventProto.getDefaultInstance());
    }

    @Test
    void toleratesUnknownFieldFromNewerWriter() {
        // A payload written by a NEWER schema version (extra field) must deserialize on an older
        // replica during a rolling deploy — the unknown field lands in the unknown-field set.
        var v2 =
                io.github.bams22.outboxer.serializer.protobuf.testproto.v2.SubscriptionChangedProto
                        .newBuilder()
                        .setSubscriptionId("sub-1")
                        .setNewPlanName("gold")
                        .build();

        var parsed =
                serializer.deserialize(
                        serializer.serialize(v2),
                        io.github.bams22.outboxer.serializer.protobuf.testproto.v1
                                .SubscriptionChangedProto.class);

        assertThat(parsed.getSubscriptionId()).isEqualTo("sub-1");
    }

    @Test
    void toleratesRemovedFieldFromOlderWriter() {
        // A payload written by an OLDER schema version (field since removed) must deserialize on a
        // newer replica — the removed field is unknown to v2 and is simply carried along.
        var v1 =
                io.github.bams22.outboxer.serializer.protobuf.testproto.v1.SubscriptionChangedProto
                        .newBuilder()
                        .setSubscriptionId("sub-2")
                        .setOldPlanCode(7)
                        .build();

        var parsed =
                serializer.deserialize(
                        serializer.serialize(v1),
                        io.github.bams22.outboxer.serializer.protobuf.testproto.v2
                                .SubscriptionChangedProto.class);

        assertThat(parsed.getSubscriptionId()).isEqualTo("sub-2");
    }

    @Test
    void preservesUnknownFieldsThroughReserialization() {
        // proto3 retains unknown fields: v2 payload read by a v1 replica and re-serialized must not
        // lose the v2-only field — this is what makes mixed-version rolling deploys safe.
        var v2 =
                io.github.bams22.outboxer.serializer.protobuf.testproto.v2.SubscriptionChangedProto
                        .newBuilder()
                        .setSubscriptionId("sub-3")
                        .setNewPlanName("platinum")
                        .build();

        var seenByV1 =
                serializer.deserialize(
                        serializer.serialize(v2),
                        io.github.bams22.outboxer.serializer.protobuf.testproto.v1
                                .SubscriptionChangedProto.class);
        var backToV2 =
                serializer.deserialize(
                        serializer.serialize(seenByV1),
                        io.github.bams22.outboxer.serializer.protobuf.testproto.v2
                                .SubscriptionChangedProto.class);

        assertThat(backToV2.getNewPlanName()).isEqualTo("platinum");
    }

    @Test
    void raisesPublishExceptionOnUnserializableInput() {
        assertThatThrownBy(() -> serializer.serialize("not-a-message"))
                .isInstanceOf(PublishSerializationException.class)
                .hasMessageContaining("java.lang.String");
    }

    @Test
    void rejectsTextLaneInput() {
        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        SerializedPayload.ofText("{}"), OrderCreatedProto.class))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void raisesDeserializationExceptionOnCorruptedBytes() {
        // Field 1, wire type 2 (length-delimited), declared length 5 but only one byte follows.
        byte[] truncated = {0x0A, 0x05, 0x01};

        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        SerializedPayload.ofBytes(truncated),
                                        OrderCreatedProto.class))
                .isInstanceOf(PayloadDeserializationException.class)
                .hasCauseInstanceOf(InvalidProtocolBufferException.class);
    }

    @Test
    void raisesDeserializationExceptionOnNonMessageTargetType() {
        SerializedPayload payload = serializer.serialize(simpleOrder());

        assertThatThrownBy(() -> serializer.deserialize(payload, String.class))
                .isInstanceOf(PayloadDeserializationException.class)
                .hasMessageContaining("java.lang.String");
    }

    @Test
    void rejectsNullExtensionRegistry() {
        assertThatThrownBy(() -> new ProtobufEventSerializer(null))
                .isInstanceOf(NullPointerException.class);
    }

    // --- fixtures ---

    private static OrderCreatedProto simpleOrder() {
        return OrderCreatedProto.newBuilder()
                .setOrderId("ord-1")
                .setCount(42)
                .setOccurredAtEpochMillis(1_776_600_000_000L)
                .setStatus(OrderStatusProto.ORDER_STATUS_NEW)
                .build();
    }
}
