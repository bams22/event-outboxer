/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.serializer.protobuf;

import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.exception.PayloadDeserializationException;
import io.github.bams22.outboxer.domain.exception.PublishSerializationException;
import io.github.bams22.outboxer.spi.EventSerializer;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Protobuf-backed {@link EventSerializer} for schema-first payloads: every payload DTO must be a
 * protoc-generated {@link Message} subclass (ADR-0026). Arbitrary POJOs are rejected at publish
 * time — there is no runtime schema generation.
 *
 * <p>Payloads travel in the bytes lane of {@link SerializedPayload} and round-trip byte-exactly
 * through storage adapters (PostgreSQL {@code BYTEA}, ADR-0025).
 *
 * <p>Deserialization resolves the generated static {@code parser()} accessor of the target class
 * once via reflection and caches the resulting {@link Parser} per class. Parsers are stateless and
 * thread-safe, so a single instance of this serializer serves all event types concurrently.
 */
public final class ProtobufEventSerializer implements EventSerializer {

  /**
   * Stable format id persisted with every event written by this serializer (ADR-0026). Never
   * renamed — stored events reference it forever.
   */
  public static final String FORMAT = "protobuf";

  private final ExtensionRegistryLite extensionRegistry;
  private final ConcurrentHashMap<Class<?>, Parser<?>> parsers = new ConcurrentHashMap<>();

  /**
   * Zero-config variant using the empty {@link ExtensionRegistryLite} — sufficient for proto3
   * messages, which have no extensions.
   */
  public ProtobufEventSerializer() {
    this(ExtensionRegistryLite.getEmptyRegistry());
  }

  /**
   * Variant for proto2 extension users: fields registered in the given registry are parsed as
   * extensions instead of landing in the unknown-field set. Mirrors how the Jackson adapter takes a
   * caller-supplied {@code ObjectMapper}.
   */
  public ProtobufEventSerializer(ExtensionRegistryLite extensionRegistry) {
    this.extensionRegistry =
        Objects.requireNonNull(extensionRegistry, "extensionRegistry must not be null");
  }

  @Override
  public String format() {
    return FORMAT;
  }

  @Override
  public SerializedPayload serialize(Object payload) {
    Objects.requireNonNull(payload, "payload must not be null");
    if (!(payload instanceof Message message)) {
      throw new PublishSerializationException(
          "payload of type "
              + payload.getClass().getName()
              + " is not a com.google.protobuf.Message — the protobuf serializer is schema-first"
              + " and accepts only protoc-generated classes (ADR-0026)",
          null);
    }
    return SerializedPayload.ofBytes(message.toByteArray());
  }

  @Override
  public <T> T deserialize(SerializedPayload payload, Class<T> type) {
    Objects.requireNonNull(payload, "payload must not be null");
    Objects.requireNonNull(type, "type must not be null");
    Parser<?> parser = parsers.computeIfAbsent(type, ProtobufEventSerializer::lookupParser);
    // Outside the try: text-lane input surfaces as IllegalStateException, not as a parse failure.
    byte[] bytes = payload.requireBytes();
    try {
      return type.cast(parser.parseFrom(bytes, extensionRegistry));
    } catch (InvalidProtocolBufferException ex) {
      throw new PayloadDeserializationException(
          "failed to parse protobuf payload as " + type.getName(), ex);
    }
  }

  private static Parser<?> lookupParser(Class<?> type) {
    if (!Message.class.isAssignableFrom(type)) {
      throw new PayloadDeserializationException(
          "target type "
              + type.getName()
              + " is not a com.google.protobuf.Message — the protobuf serializer is schema-first"
              + " and deserializes only protoc-generated classes (ADR-0026)",
          null);
    }
    try {
      Method parserAccessor = type.getMethod("parser");
      return (Parser<?>) parserAccessor.invoke(null);
    } catch (ReflectiveOperationException | ClassCastException ex) {
      throw new PayloadDeserializationException(
          "failed to obtain a protobuf Parser via static " + type.getName() + ".parser()", ex);
    }
  }
}
