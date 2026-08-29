/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.api.publish;

import io.github.bams22.outboxer.domain.EventType;
import java.util.Objects;
import lombok.Builder;
import org.jspecify.annotations.Nullable;

/**
 * Single request in a batch {@link OutboxEventPublisher#publishAll(java.util.Collection)}
 * invocation. Carries the same data as a scalar {@code publish(...)} call: the typed event key, the
 * payload, and optional overrides.
 *
 * @param type typed event key (ADR-0031); its name is validated by {@link EventType} itself
 * @param payload DTO payload (not null); must be an instance of {@code type.payloadType()}, which
 *     the publisher checks
 * @param options optional per-request overrides; {@code null} is treated as {@link
 *     PublishOptions#defaults()}
 * @param <T> payload type
 */
@Builder
public record PublishRequest<T>(EventType<T> type, T payload, @Nullable PublishOptions options) {

    public PublishRequest {
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(payload, "payload must not be null");
    }

    /** Request with default options. */
    public static <T> PublishRequest<T> of(EventType<T> type, T payload) {
        return new PublishRequest<>(type, payload, null);
    }

    /** Request with explicit options ({@code null} = defaults). */
    public static <T> PublishRequest<T> of(
            EventType<T> type, T payload, @Nullable PublishOptions options) {
        return new PublishRequest<>(type, payload, options);
    }
}
