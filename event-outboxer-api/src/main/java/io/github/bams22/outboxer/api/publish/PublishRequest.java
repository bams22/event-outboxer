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

import java.util.Objects;
import lombok.Builder;
import org.jspecify.annotations.Nullable;

/**
 * Single request in a batch {@link OutboxEventPublisher#publishAll(java.util.Collection)}
 * invocation. Carries the same data as a scalar {@code publish(...)} call: event type, payload,
 * and optional overrides.
 *
 * @param eventType non-blank event-type identifier
 * @param payload DTO payload (not null)
 * @param options optional per-request overrides; {@code null} is treated as
 *     {@link PublishOptions#defaults()}
 */
@Builder
public record PublishRequest(String eventType, Object payload, @Nullable PublishOptions options) {

  public PublishRequest {
    Objects.requireNonNull(eventType, "eventType must not be null");
    if (eventType.isBlank()) {
      throw new IllegalArgumentException("eventType must not be blank");
    }
    Objects.requireNonNull(payload, "payload must not be null");
  }
}
