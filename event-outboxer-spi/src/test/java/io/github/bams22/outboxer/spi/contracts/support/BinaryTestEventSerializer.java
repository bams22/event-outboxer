/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spi.contracts.support;

import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.exception.PayloadDeserializationException;
import io.github.bams22.outboxer.domain.exception.PublishSerializationException;
import io.github.bams22.outboxer.spi.EventSerializer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Deliberately binary {@link EventSerializer} for tests (ADR-0025): encodes {@link
 * BinaryTestPayload} via {@link DataOutputStream} behind a two-byte magic prefix {@code 0x00 0xFF}
 * — an invalid UTF-8 sequence — so any component that squeezes the payload through a text codepath
 * fails loudly instead of silently corrupting it.
 */
public final class BinaryTestEventSerializer implements EventSerializer {

    /** Stable format id persisted with events written by this serializer. */
    public static final String FORMAT = "test-binary";

    private static final byte[] MAGIC = {0x00, (byte) 0xFF};

    @Override
    public String format() {
        return FORMAT;
    }

    @Override
    public SerializedPayload serialize(Object payload) {
        if (!(payload instanceof BinaryTestPayload dto)) {
            throw new PublishSerializationException(
                    "BinaryTestEventSerializer only encodes BinaryTestPayload, got "
                            + payload.getClass().getName(),
                    null);
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataOutputStream data = new DataOutputStream(out)) {
            data.write(MAGIC);
            data.writeUTF(dto.name());
            data.writeInt(dto.number());
        } catch (IOException e) {
            throw new PublishSerializationException("failed to encode BinaryTestPayload", e);
        }
        return SerializedPayload.ofBytes(out.toByteArray());
    }

    @Override
    public <T> T deserialize(SerializedPayload payload, Class<T> type) {
        if (!type.isAssignableFrom(BinaryTestPayload.class)) {
            throw new PayloadDeserializationException(
                    "BinaryTestEventSerializer only decodes BinaryTestPayload, asked for "
                            + type.getName(),
                    null);
        }
        byte[] bytes = payload.requireBytes();
        try (DataInputStream data = new DataInputStream(new ByteArrayInputStream(bytes))) {
            byte[] magic = new byte[MAGIC.length];
            data.readFully(magic);
            if (magic[0] != MAGIC[0] || magic[1] != MAGIC[1]) {
                throw new PayloadDeserializationException("bad magic prefix", null);
            }
            BinaryTestPayload dto = new BinaryTestPayload(data.readUTF(), data.readInt());
            return type.cast(dto);
        } catch (IOException e) {
            throw new PayloadDeserializationException("failed to decode BinaryTestPayload", e);
        }
    }
}
