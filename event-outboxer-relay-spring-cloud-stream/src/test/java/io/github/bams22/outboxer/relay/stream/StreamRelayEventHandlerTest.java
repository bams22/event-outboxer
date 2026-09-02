/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.relay.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.domain.WorkerId;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.cloud.stream.function.StreamOperations;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

class StreamRelayEventHandlerTest {

    private StreamOperations streamOperations;

    @BeforeEach
    void setUp() {
        streamOperations = mock(StreamOperations.class);
        when(streamOperations.send(any(String.class), any())).thenReturn(true);
    }

    private static EventContext context() {
        return EventContext.builder()
                .eventId(UUID.randomUUID())
                .eventType(StreamEnvelope.EVENT_TYPE_NAME)
                .attempt(1)
                .createdAt(Instant.now())
                .claimedAt(Instant.now())
                .workerId(WorkerId.generateDefault())
                .traceContext(Map.of())
                .build();
    }

    private static StreamEnvelope.StreamEnvelopeBuilder envelope() {
        return StreamEnvelope.builder()
                .binding("orders-out")
                .key("k1")
                .headers(Map.of("x-app", "1"))
                .contentType("application/json")
                .textPayload("{\"a\":1}");
    }

    private StreamRelayEventHandler defaultHandler() {
        return StreamRelayEventHandler.builder().streamOperations(streamOperations).build();
    }

    private Message<?> handleAndCaptureMessage(StreamRelayEventHandler handler, StreamEnvelope e) {
        EventOutcome outcome = handler.handle(context(), e);
        assertThat(outcome).isInstanceOf(EventOutcome.Success.class);
        ArgumentCaptor<Message<?>> captor = ArgumentCaptor.forClass(Message.class);
        verify(streamOperations).send(eq(e.binding()), captor.capture());
        return captor.getValue();
    }

    @Test
    void sendsTextPayloadWithAssembledHeaders() {
        Message<?> message = handleAndCaptureMessage(defaultHandler(), envelope().build());

        assertThat(message.getPayload()).isEqualTo("{\"a\":1}");
        assertThat(message.getHeaders())
                .containsEntry("x-app", "1")
                .containsEntry(MessageHeaders.CONTENT_TYPE, "application/json");
        assertThat((byte[]) message.getHeaders().get("kafka_messageKey"))
                .isEqualTo("k1".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void sendsBinaryPayloadVerbatim() {
        byte[] bytes = new byte[] {1, 2, 3};
        Message<?> message =
                handleAndCaptureMessage(
                        defaultHandler(),
                        envelope().textPayload(null).binaryPayload(bytes).build());

        assertThat(message.getPayload()).isEqualTo(bytes);
    }

    @Test
    void explicitContentTypeWinsOverCopiedHeader() {
        Message<?> message =
                handleAndCaptureMessage(
                        defaultHandler(),
                        envelope()
                                .headers(Map.of(MessageHeaders.CONTENT_TYPE, "text/plain"))
                                .build());

        assertThat(message.getHeaders())
                .containsEntry(MessageHeaders.CONTENT_TYPE, "application/json");
    }

    @Test
    void customKeyHeaderNameIsUsed() {
        StreamRelayEventHandler handler =
                StreamRelayEventHandler.builder()
                        .streamOperations(streamOperations)
                        .messageKeyHeader("myKey")
                        .build();

        Message<?> message = handleAndCaptureMessage(handler, envelope().build());

        assertThat((byte[]) message.getHeaders().get("myKey"))
                .isEqualTo("k1".getBytes(StandardCharsets.UTF_8));
        assertThat(message.getHeaders()).doesNotContainKey("kafka_messageKey");
    }

    @Test
    void blankKeyHeaderConfigDisablesTheHeader() {
        StreamRelayEventHandler handler =
                StreamRelayEventHandler.builder()
                        .streamOperations(streamOperations)
                        .messageKeyHeader("")
                        .build();

        Message<?> message = handleAndCaptureMessage(handler, envelope().build());

        assertThat(message.getHeaders()).doesNotContainKey("kafka_messageKey");
    }

    @Test
    void nullKeyMeansNoKeyHeader() {
        Message<?> message =
                handleAndCaptureMessage(defaultHandler(), envelope().key(null).build());

        assertThat(message.getHeaders()).doesNotContainKey("kafka_messageKey");
    }

    @Test
    void falseFromSendBecomesRetry() {
        when(streamOperations.send(any(String.class), any())).thenReturn(false);

        EventOutcome outcome = defaultHandler().handle(context(), envelope().build());

        assertThat(outcome)
                .isInstanceOfSatisfying(
                        EventOutcome.Retry.class,
                        retry -> assertThat(retry.reason()).contains("orders-out"));
    }

    @Test
    void sendExceptionPropagatesToTheEngine() {
        when(streamOperations.send(any(String.class), any()))
                .thenThrow(new IllegalStateException("binder down"));

        assertThatThrownBy(() -> defaultHandler().handle(context(), envelope().build()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("binder down");
    }

    @Test
    void lockKeyIsNullByDefault() {
        assertThat(defaultHandler().extractLockKey(envelope().build())).isNull();
    }

    @Test
    void lockKeyIsNamespacedWhenOrderingEnabled() {
        StreamRelayEventHandler handler =
                StreamRelayEventHandler.builder()
                        .streamOperations(streamOperations)
                        .perKeyOrdering(true)
                        .build();

        assertThat(handler.extractLockKey(envelope().build()))
                .isEqualTo("outboxer-stream-relay:orders-out:k1");
        assertThat(handler.extractLockKey(envelope().key(null).build())).isNull();
    }

    @Test
    void registersUnderTheRelayEventType() {
        assertThat(defaultHandler().type()).isEqualTo(StreamEnvelope.EVENT_TYPE);
        assertThat(defaultHandler().eventType()).isEqualTo("outboxer-stream-relay");
    }
}
