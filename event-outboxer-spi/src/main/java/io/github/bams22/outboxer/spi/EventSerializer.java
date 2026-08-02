/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spi;

import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.exception.PayloadDeserializationException;
import io.github.bams22.outboxer.domain.exception.PublishSerializationException;

/**
 * Port that converts handler payloads to and from the serialized form persisted in the outbox.
 *
 * <p>The publisher calls {@link #serialize(Object)} before handing the event to {@code
 * EventStore.save(...)} and records {@link #format()} alongside the payload; the dispatcher selects
 * the serializer whose {@code format()} matches the stored value (via {@code
 * EventSerializerRegistry}) and calls {@link #deserialize(SerializedPayload, Class)} before
 * invoking {@code EventHandler.handle(ctx, payload)}. The engine itself never inspects the
 * serialized form — it only copies it verbatim through the storage layer (see ADR-0011, ADR-0025).
 *
 * <p>The target class is always the handler's declared {@code EventHandler.payloadType()} — the
 * FQCN stored with the event is diagnostics only and is never used to resolve a type.
 *
 * <p>Textual formats (JSON) should return {@link SerializedPayload#ofText(String)}: storage
 * adapters may then persist them in a structured column (PostgreSQL {@code JSONB}), which keeps
 * them readable and indexable but canonicalizes whitespace and key order — round-trips are
 * semantic, not byte-identical. Binary formats must return {@link
 * SerializedPayload#ofBytes(byte[])} and get byte-exact round-trips.
 *
 * <p>Implementations must be thread-safe; the engine shares a single instance across all event
 * types.
 */
public interface EventSerializer {

    /**
     * Stable identifier of this serializer's wire format, for example {@code "jackson-json"}.
     *
     * <p>The value is persisted with every event and used to select the deserializer when the event
     * is handled — possibly by a different process, release, or configuration. It must be lowercase
     * kebab-case, at most 64 characters, and must never be renamed once events written with it may
     * exist (ADR-0025).
     */
    String format();

    /**
     * Serialize a payload DTO to the form stored in the outbox.
     *
     * @throws PublishSerializationException if the payload cannot be represented in the
     *     serializer's format
     */
    SerializedPayload serialize(Object payload);

    /**
     * Reconstruct a payload DTO from its stored form.
     *
     * @throws PayloadDeserializationException if the stored payload cannot be parsed into {@code
     *     type}
     */
    <T> T deserialize(SerializedPayload payload, Class<T> type);
}
