/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.target;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * What the driver asks a target to publish. Targets map it onto their own payload type; the handler
 * must report {@code seq} and {@code lockKey} back to the ledger unchanged.
 *
 * @param seq sequence number, unique within the run
 * @param typeIndex which of the scenario's event types this belongs to
 * @param lockKey lock key the handler should return, {@code null} for none
 * @param padding filler that brings the serialized payload to the scenario's size
 */
public record BenchmarkEvent(long seq, int typeIndex, @Nullable String lockKey, String padding) {

    public BenchmarkEvent {
        Objects.requireNonNull(padding, "padding must not be null");
        if (seq < 0) {
            throw new IllegalArgumentException("seq must be >= 0, got " + seq);
        }
        if (typeIndex < 0) {
            throw new IllegalArgumentException("typeIndex must be >= 0, got " + typeIndex);
        }
    }
}
