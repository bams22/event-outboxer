/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.ledger;

import java.time.Instant;
import java.util.Objects;
import lombok.Builder;
import org.jspecify.annotations.Nullable;

/**
 * One handler invocation as the handler itself saw it. Recorded for every attempt, successful or
 * not, so retries and duplicates are visible.
 *
 * @param seq the event's benchmark sequence number
 * @param eventType the event type name
 * @param attempt the library's attempt counter, 1-based
 * @param workerId worker that ran the handler
 * @param thread handler thread name (or id for unnamed virtual threads)
 * @param lockKey lock key the handler returned, {@code null} when keys are off
 * @param startedAt wall-clock start of the handler body
 * @param finishedAt wall-clock end of the handler body, before the outcome is returned
 * @param outcome what the handler returned
 */
@Builder
public record Handling(
        long seq,
        String eventType,
        int attempt,
        String workerId,
        String thread,
        @Nullable String lockKey,
        Instant startedAt,
        Instant finishedAt,
        Outcome outcome) {

    public Handling {
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(workerId, "workerId must not be null");
        Objects.requireNonNull(thread, "thread must not be null");
        Objects.requireNonNull(startedAt, "startedAt must not be null");
        Objects.requireNonNull(finishedAt, "finishedAt must not be null");
        Objects.requireNonNull(outcome, "outcome must not be null");
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1, got " + attempt);
        }
        if (finishedAt.isBefore(startedAt)) {
            throw new IllegalArgumentException("finishedAt must not precede startedAt");
        }
    }

    /** Whether this invocation counted as the event being handled. */
    public boolean succeeded() {
        return outcome == Outcome.SUCCESS;
    }

    /**
     * The handler's verdict, mirroring the library's {@code EventOutcome} shapes the harness uses.
     */
    public enum Outcome {
        SUCCESS,
        RETRY,
        FAIL
    }
}
