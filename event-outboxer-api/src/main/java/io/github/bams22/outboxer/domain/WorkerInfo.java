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

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import lombok.Builder;
import org.jspecify.annotations.Nullable;

/**
 * Metadata supplied when a worker registers itself through {@code WorkerRegistry.register(...)}.
 * Persisted as-is by the adapter (the PostgreSQL implementation stores {@code metadata} as a JSONB
 * column).
 *
 * @param id worker identifier (mandatory)
 * @param host hostname running the worker (mandatory)
 * @param pid operating-system process id; optional
 * @param startedAt wall-clock time the worker started; optional — the registry may default to
 *     storage-side {@code now()}
 * @param metadata arbitrary key/value tags such as application name, version, git SHA, or
 *     deployment environment; never null (empty map allowed)
 */
@Builder
public record WorkerInfo(
    WorkerId id,
    String host,
    @Nullable Integer pid,
    @Nullable Instant startedAt,
    Map<String, String> metadata) {

  public WorkerInfo {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(host, "host must not be null");
    if (host.isBlank()) {
      throw new IllegalArgumentException("host must not be blank");
    }
    Objects.requireNonNull(metadata, "metadata must not be null");
    metadata = Map.copyOf(metadata);
  }
}
