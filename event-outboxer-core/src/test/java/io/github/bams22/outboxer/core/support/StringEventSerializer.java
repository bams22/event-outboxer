/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.support;

import io.github.bams22.outboxer.domain.exception.PayloadDeserializationException;
import io.github.bams22.outboxer.domain.exception.PublishSerializationException;
import io.github.bams22.outboxer.spi.EventSerializer;

/**
 * Test-only {@link EventSerializer} that only accepts {@code String} payloads and round-trips
 * them verbatim. Used by core unit / integration tests to avoid pulling in the Jackson adapter.
 */
public final class StringEventSerializer implements EventSerializer {

  @Override
  public String serialize(Object payload) {
    if (payload instanceof String s) {
      return s;
    }
    throw new PublishSerializationException(
        "StringEventSerializer only supports String payloads, got "
            + payload.getClass().getName(),
        new IllegalArgumentException());
  }

  @Override
  public <T> T deserialize(String payload, Class<T> type) {
    if (type == String.class) {
      @SuppressWarnings("unchecked")
      T cast = (T) payload;
      return cast;
    }
    throw new PayloadDeserializationException(
        "StringEventSerializer only supports String payloads, got " + type.getName(),
        new IllegalArgumentException());
  }
}
