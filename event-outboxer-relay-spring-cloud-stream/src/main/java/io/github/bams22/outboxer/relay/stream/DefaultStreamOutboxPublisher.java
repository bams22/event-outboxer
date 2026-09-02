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

import io.github.bams22.outboxer.api.publish.OutboxEventPublisher;
import io.github.bams22.outboxer.api.publish.PublishRequest;
import io.github.bams22.outboxer.domain.SerializedPayload;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import lombok.Builder;
import org.jspecify.annotations.Nullable;

/**
 * Default {@link StreamOutboxPublisher}: encodes the payload, wraps it into a {@link
 * StreamEnvelope} and stores it through the engine's {@code OutboxEventPublisher} under {@link
 * StreamEnvelope#EVENT_TYPE}.
 *
 * <p><b>Construction.</b> {@code DefaultStreamOutboxPublisher.builder()} — see the constructor for
 * required collaborators and defaults.
 */
public final class DefaultStreamOutboxPublisher implements StreamOutboxPublisher {

    private final OutboxEventPublisher outboxEventPublisher;
    private final StreamPayloadEncoder encoder;
    private final String defaultContentType;

    /**
     * Builder-backed constructor; parameter names are the builder's method names. Required: {@code
     * outboxEventPublisher} and {@code encoder}. Defaults: {@code defaultContentType} — {@link
     * StreamPayloadEncoder#DEFAULT_CONTENT_TYPE} (used for pre-encoded payloads without an explicit
     * content type).
     */
    @Builder
    private DefaultStreamOutboxPublisher(
            OutboxEventPublisher outboxEventPublisher,
            StreamPayloadEncoder encoder,
            @Nullable String defaultContentType) {
        this.outboxEventPublisher =
                Objects.requireNonNull(
                        outboxEventPublisher, "outboxEventPublisher must not be null");
        this.encoder = Objects.requireNonNull(encoder, "encoder must not be null");
        this.defaultContentType =
                defaultContentType != null
                        ? defaultContentType
                        : StreamPayloadEncoder.DEFAULT_CONTENT_TYPE;
    }

    @Override
    public UUID publish(String binding, @Nullable String key, Object payload) {
        return publish(StreamOutboxMessage.of(binding, key, payload));
    }

    @Override
    public UUID publish(StreamOutboxMessage message) {
        Objects.requireNonNull(message, "message must not be null");
        return outboxEventPublisher.publish(
                StreamEnvelope.EVENT_TYPE, toEnvelope(message), message.options());
    }

    @Override
    public List<UUID> publishAll(Collection<StreamOutboxMessage> messages) {
        Objects.requireNonNull(messages, "messages must not be null");
        return outboxEventPublisher.publishAll(
                messages.stream()
                        .map(
                                message ->
                                        PublishRequest.of(
                                                StreamEnvelope.EVENT_TYPE,
                                                toEnvelope(message),
                                                message.options()))
                        .toList());
    }

    private StreamEnvelope toEnvelope(StreamOutboxMessage message) {
        SerializedPayload wire;
        boolean encoded = false;
        switch (message.payload()) {
            case SerializedPayload preEncoded -> wire = preEncoded;
            case String text -> wire = SerializedPayload.ofText(text);
            case byte[] bytes -> wire = SerializedPayload.ofBytes(bytes);
            default -> {
                wire = encoder.encode(message.payload());
                encoded = true;
            }
        }
        String contentType =
                message.contentType() != null
                        ? message.contentType()
                        : encoded ? encoder.contentType() : defaultContentType;
        return StreamEnvelope.builder()
                .binding(message.binding())
                .key(message.key())
                .headers(message.headers())
                .contentType(contentType)
                .textPayload(wire.text())
                .binaryPayload(wire.bytes())
                .build();
    }
}
