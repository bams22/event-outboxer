/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.domain;

import java.util.Arrays;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * The serialized form of an event payload as produced by {@code EventSerializer.serialize(...)} and
 * carried verbatim through the storage layer (ADR-0025).
 *
 * <p>A payload travels in exactly one of two lanes:
 *
 * <ul>
 *   <li><b>text</b> — for textual formats such as JSON. Storage adapters may persist it in a
 *       structured column (PostgreSQL {@code JSONB}), which keeps it readable in {@code psql} and
 *       indexable, but may canonicalize whitespace and key order — textual round-trips are
 *       semantically, not byte-identically, stable.
 *   <li><b>bytes</b> — for binary formats such as Protobuf. Storage adapters persist it verbatim
 *       (PostgreSQL {@code BYTEA}); binary round-trips are byte-exact.
 * </ul>
 *
 * <p>Exactly one of {@link #text()} / {@link #bytes()} is non-null; the compact constructor
 * enforces this. The pipeline passes instances through without copying — callers must not mutate
 * the {@code bytes} array after handing it over.
 *
 * @param text textual serialized form; {@code null} for binary payloads
 * @param bytes binary serialized form; {@code null} for textual payloads
 */
public record SerializedPayload(@Nullable String text, byte @Nullable [] bytes) {

    public SerializedPayload {
        if ((text == null) == (bytes == null)) {
            throw new IllegalArgumentException("exactly one of text/bytes must be non-null");
        }
    }

    /** Wrap a textual serialized form. */
    public static SerializedPayload ofText(String text) {
        Objects.requireNonNull(text, "text must not be null");
        return new SerializedPayload(text, null);
    }

    /** Wrap a binary serialized form. The array is not copied — the caller must not mutate it. */
    public static SerializedPayload ofBytes(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes must not be null");
        return new SerializedPayload(null, bytes);
    }

    /** Whether this payload travels in the text lane. */
    public boolean isText() {
        return text != null;
    }

    /**
     * The textual form, or throw if this is a binary payload.
     *
     * @throws IllegalStateException if the payload is binary
     */
    public String requireText() {
        if (text == null) {
            throw new IllegalStateException("binary payload — use bytes()/requireBytes()");
        }
        return text;
    }

    /**
     * The binary form, or throw if this is a textual payload.
     *
     * @throws IllegalStateException if the payload is textual
     */
    public byte[] requireBytes() {
        if (bytes == null) {
            throw new IllegalStateException("textual payload — use text()/requireText()");
        }
        return bytes;
    }

    /** Value equality; the byte lane compares array contents, not identity. */
    @Override
    public boolean equals(@Nullable Object obj) {
        return obj instanceof SerializedPayload other
                && Objects.equals(text, other.text)
                && Arrays.equals(bytes, other.bytes);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hashCode(text) + Arrays.hashCode(bytes);
    }

    /** Never dumps payload content — text length or byte count only (log hygiene). */
    @Override
    public String toString() {
        return text != null
                ? "SerializedPayload[text,length=" + text.length() + "]"
                : "SerializedPayload[bytes,length=" + requireBytes().length + "]";
    }
}
