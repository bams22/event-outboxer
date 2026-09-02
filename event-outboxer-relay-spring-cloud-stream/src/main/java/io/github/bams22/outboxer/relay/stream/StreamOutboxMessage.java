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

import io.github.bams22.outboxer.api.publish.PublishOptions;
import java.util.Map;
import java.util.Objects;
import lombok.Builder;
import org.jspecify.annotations.Nullable;

/**
 * A message to publish through {@link StreamOutboxPublisher}: the full form of a relay request, for
 * callers that need headers, an explicit content type or per-call {@code PublishOptions} on top of
 * the (binding, key, payload) triple.
 *
 * <p>The payload is passed raw here; the publisher encodes it to its wire form at publish time (see
 * {@link StreamOutboxPublisher} for the lane rules on {@code String}, {@code byte[]} and {@code
 * SerializedPayload} payloads).
 *
 * @param binding Spring Cloud Stream binding name to deliver the message to
 * @param key optional message key (for example an aggregate id)
 * @param headers broker message headers to copy onto the outgoing message; never {@code null}
 *     (normalized to an empty map)
 * @param payload the message payload; an explicit DTO (ADR-0003), or a pre-encoded {@code String} /
 *     {@code byte[]} / {@code SerializedPayload} wire form
 * @param contentType MIME type override for the wire payload; {@code null} to use the encoder's
 *     content type (or the configured default for pre-encoded payloads)
 * @param options per-call outbox tuning ({@code runAt}, {@code dedupKey}, ...); {@code null} for
 *     defaults
 */
@Builder
public record StreamOutboxMessage(
        String binding,
        @Nullable String key,
        Map<String, String> headers,
        Object payload,
        @Nullable String contentType,
        @Nullable PublishOptions options) {

    public StreamOutboxMessage {
        Objects.requireNonNull(binding, "binding must not be null");
        if (binding.isBlank()) {
            throw new IllegalArgumentException("binding must not be blank");
        }
        headers = headers == null ? Map.of() : Map.copyOf(headers);
        Objects.requireNonNull(payload, "payload must not be null");
        if (contentType != null && contentType.isBlank()) {
            throw new IllegalArgumentException("contentType must not be blank when set");
        }
    }

    /**
     * Shorthand for the common case — binding, key and payload with no extra headers or options.
     */
    public static StreamOutboxMessage of(String binding, @Nullable String key, Object payload) {
        return StreamOutboxMessage.builder().binding(binding).key(key).payload(payload).build();
    }
}
