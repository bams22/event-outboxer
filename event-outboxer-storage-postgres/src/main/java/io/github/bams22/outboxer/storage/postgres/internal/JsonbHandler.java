/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.storage.postgres.internal;

import java.sql.SQLException;
import java.util.Objects;
import org.jspecify.annotations.Nullable;
import org.postgresql.util.PGobject;

/**
 * Converts {@code String} JSON payloads to and from {@link PGobject} with type {@code jsonb} so
 * that {@link java.sql.PreparedStatement#setObject(int, Object)} binds them correctly. The
 * outbox persists two JSONB columns — {@code payload} and {@code trace_context} — both of which
 * arrive as already-serialized JSON strings from the {@code EventSerializer}.
 */
public final class JsonbHandler {

  private JsonbHandler() {}

  /** Wrap a non-null JSON string into a {@code jsonb} {@link PGobject}. */
  public static PGobject jsonb(String json) {
    Objects.requireNonNull(json, "json must not be null");
    PGobject obj = new PGobject();
    obj.setType("jsonb");
    try {
      obj.setValue(json);
    } catch (SQLException e) {
      // PGobject.setValue throws checked SQLException only by signature; the default
      // implementation never actually throws.
      throw new IllegalStateException("failed to set jsonb value", e);
    }
    return obj;
  }

  /** Wrap a nullable JSON string; returns {@code null} if the input is {@code null}. */
  public static @Nullable PGobject jsonbNullable(@Nullable String json) {
    return json == null ? null : jsonb(json);
  }
}
