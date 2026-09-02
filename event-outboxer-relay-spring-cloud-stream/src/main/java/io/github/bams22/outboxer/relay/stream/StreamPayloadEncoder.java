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

import io.github.bams22.outboxer.domain.SerializedPayload;

/**
 * Encodes a user payload to its broker wire form at publish time (ADR-0032). The result is stored
 * inside the {@link StreamEnvelope} and shipped verbatim by the built-in handler — the encoder runs
 * in the caller's transaction, so encoding failures fail the publish, not the delivery.
 *
 * <p>The default implementation is {@link JacksonStreamPayloadEncoder} (JSON via the resolved
 * {@code ObjectMapper}). Applications using a different wire format (Avro, Protobuf, ...) replace
 * it with their own bean; the encoder is only consulted for payloads that are not already
 * pre-encoded ({@code String}, {@code byte[]} or {@code SerializedPayload} pass through).
 */
public interface StreamPayloadEncoder {

    /** Content type used when nothing more specific is configured or reported: {@value}. */
    String DEFAULT_CONTENT_TYPE = "application/json";

    /**
     * Encode the payload to its wire form.
     *
     * @throws StreamEncodingException if the payload cannot be encoded
     */
    SerializedPayload encode(Object payload);

    /**
     * MIME type of the wire form this encoder produces; stamped on the envelope (and the outgoing
     * message) unless the caller overrides it per message.
     */
    default String contentType() {
        return DEFAULT_CONTENT_TYPE;
    }
}
