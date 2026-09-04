/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.target.outboxer;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * The explicit DTO (ADR-0003) the event-outboxer target stores. Mirrors {@code BenchmarkEvent}
 * one-to-one so the handler can report the sequence number and lock key back verbatim.
 *
 * @param seq benchmark sequence number
 * @param lockKey lock key to return from {@code extractLockKey}, {@code null} for none
 * @param padding size filler
 */
public record BenchPayload(long seq, @Nullable String lockKey, String padding) {

    public BenchPayload {
        Objects.requireNonNull(padding, "padding must not be null");
    }
}
