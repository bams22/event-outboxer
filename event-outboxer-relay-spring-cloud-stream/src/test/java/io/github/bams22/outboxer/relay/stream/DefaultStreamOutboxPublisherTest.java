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
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.github.bams22.outboxer.api.publish.OutboxEventPublisher;
import io.github.bams22.outboxer.api.publish.PublishOptions;
import io.github.bams22.outboxer.api.publish.PublishRequest;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.serializer.jackson.JacksonObjectMapperFactory;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class DefaultStreamOutboxPublisherTest {

    private OutboxEventPublisher outboxEventPublisher;
    private DefaultStreamOutboxPublisher publisher;

    private record Payload(String orderId) {}

    @BeforeEach
    void setUp() {
        outboxEventPublisher = mock(OutboxEventPublisher.class);
        publisher =
                DefaultStreamOutboxPublisher.builder()
                        .outboxEventPublisher(outboxEventPublisher)
                        .encoder(
                                new JacksonStreamPayloadEncoder(
                                        JacksonObjectMapperFactory.defaults()))
                        .build();
    }

    private StreamEnvelope publishAndCaptureEnvelope(StreamOutboxMessage message) {
        publisher.publish(message);
        ArgumentCaptor<StreamEnvelope> captor = ArgumentCaptor.forClass(StreamEnvelope.class);
        verify(outboxEventPublisher)
                .publish(
                        eq(StreamEnvelope.EVENT_TYPE),
                        captor.capture(),
                        nullable(PublishOptions.class));
        return captor.getValue();
    }

    @Test
    void encodesDtoAndStoresEnvelopeUnderRelayEventType() {
        UUID id = UUID.randomUUID();
        when(outboxEventPublisher.publish(
                        eq(StreamEnvelope.EVENT_TYPE), any(), nullable(PublishOptions.class)))
                .thenReturn(id);

        UUID result = publisher.publish("orders-out", "k1", new Payload("o-1"));

        assertThat(result).isEqualTo(id);
        ArgumentCaptor<StreamEnvelope> captor = ArgumentCaptor.forClass(StreamEnvelope.class);
        verify(outboxEventPublisher)
                .publish(
                        eq(StreamEnvelope.EVENT_TYPE),
                        captor.capture(),
                        nullable(PublishOptions.class));
        StreamEnvelope envelope = captor.getValue();
        assertThat(envelope.binding()).isEqualTo("orders-out");
        assertThat(envelope.key()).isEqualTo("k1");
        assertThat(envelope.contentType()).isEqualTo("application/json");
        assertThat(envelope.textPayload()).contains("\"orderId\":\"o-1\"");
    }

    @Test
    void stringPayloadPassesThroughUnquoted() {
        StreamEnvelope envelope =
                publishAndCaptureEnvelope(StreamOutboxMessage.of("b", null, "{\"raw\":true}"));

        assertThat(envelope.textPayload()).isEqualTo("{\"raw\":true}");
    }

    @Test
    void byteArrayPayloadPassesThroughToBinaryLane() {
        byte[] bytes = "wire".getBytes(StandardCharsets.UTF_8);

        StreamEnvelope envelope =
                publishAndCaptureEnvelope(StreamOutboxMessage.of("b", null, bytes));

        assertThat(envelope.binaryPayload()).isEqualTo(bytes);
        assertThat(envelope.textPayload()).isNull();
    }

    @Test
    void serializedPayloadPassesThroughAsIs() {
        StreamEnvelope envelope =
                publishAndCaptureEnvelope(
                        StreamOutboxMessage.of("b", null, SerializedPayload.ofText("t")));

        assertThat(envelope.textPayload()).isEqualTo("t");
    }

    @Test
    void explicitContentTypeWinsOverEncoder() {
        StreamEnvelope envelope =
                publishAndCaptureEnvelope(
                        StreamOutboxMessage.builder()
                                .binding("b")
                                .payload(new Payload("o-1"))
                                .contentType("application/vnd.custom+json")
                                .build());

        assertThat(envelope.contentType()).isEqualTo("application/vnd.custom+json");
    }

    @Test
    void configuredDefaultContentTypeAppliesToPreEncodedPayloads() {
        DefaultStreamOutboxPublisher custom =
                DefaultStreamOutboxPublisher.builder()
                        .outboxEventPublisher(outboxEventPublisher)
                        .encoder(
                                new JacksonStreamPayloadEncoder(
                                        JacksonObjectMapperFactory.defaults()))
                        .defaultContentType("application/octet-stream")
                        .build();

        custom.publish(StreamOutboxMessage.of("b", null, new byte[] {1}));

        ArgumentCaptor<StreamEnvelope> captor = ArgumentCaptor.forClass(StreamEnvelope.class);
        verify(outboxEventPublisher)
                .publish(
                        eq(StreamEnvelope.EVENT_TYPE),
                        captor.capture(),
                        nullable(PublishOptions.class));
        assertThat(captor.getValue().contentType()).isEqualTo("application/octet-stream");
    }

    @Test
    void headersAndOptionsArePassedThrough() {
        PublishOptions options = PublishOptions.builder().dedupKey("dk").build();

        publisher.publish(
                StreamOutboxMessage.builder()
                        .binding("b")
                        .payload(new Payload("o-1"))
                        .headers(Map.of("x-app", "1"))
                        .options(options)
                        .build());

        ArgumentCaptor<StreamEnvelope> captor = ArgumentCaptor.forClass(StreamEnvelope.class);
        verify(outboxEventPublisher)
                .publish(eq(StreamEnvelope.EVENT_TYPE), captor.capture(), eq(options));
        assertThat(captor.getValue().headers()).containsExactlyEntriesOf(Map.of("x-app", "1"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void publishAllMapsEveryMessageInOrder() {
        List<UUID> ids = List.of(UUID.randomUUID(), UUID.randomUUID());
        when(outboxEventPublisher.publishAll(any())).thenReturn(ids);

        List<UUID> result =
                publisher.publishAll(
                        List.of(
                                StreamOutboxMessage.of("b1", "k1", new Payload("o-1")),
                                StreamOutboxMessage.of("b2", "k2", new Payload("o-2"))));

        assertThat(result).isEqualTo(ids);
        ArgumentCaptor<Collection<PublishRequest<?>>> captor =
                ArgumentCaptor.forClass(Collection.class);
        verify(outboxEventPublisher).publishAll(captor.capture());
        List<PublishRequest<?>> requests = List.copyOf(captor.getValue());
        assertThat(requests).hasSize(2);
        assertThat(requests.get(0).type()).isEqualTo(StreamEnvelope.EVENT_TYPE);
        assertThat(((StreamEnvelope) requests.get(0).payload()).binding()).isEqualTo("b1");
        assertThat(((StreamEnvelope) requests.get(1).payload()).binding()).isEqualTo("b2");
    }

    @Test
    void encodingFailurePropagatesBeforeAnythingIsPublished() {
        Object selfReferential =
                new Object() {
                    @SuppressWarnings("unused")
                    public Object getSelf() {
                        return this;
                    }
                };

        assertThatThrownBy(() -> publisher.publish("b", null, selfReferential))
                .isInstanceOf(StreamEncodingException.class);
        verifyNoInteractions(outboxEventPublisher);
    }

    @Test
    void builderRequiresCollaborators() {
        assertThatNullPointerException()
                .isThrownBy(() -> DefaultStreamOutboxPublisher.builder().build())
                .withMessageContaining("outboxEventPublisher must not be null");
    }
}
