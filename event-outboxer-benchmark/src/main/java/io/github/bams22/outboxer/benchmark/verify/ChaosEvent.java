/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.verify;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Something the harness did to the system on purpose, with enough context to explain anomalies in
 * the ledger afterwards.
 *
 * @param kind what happened
 * @param at wall-clock moment the action was taken (driver clock)
 * @param progress distinct events handled when it fired
 * @param workerIds workers involved: killed or spawned; empty for a database restart
 * @param details free text for the report (restart mode, time until the database answered, ...)
 */
public record ChaosEvent(
        Kind kind, Instant at, long progress, List<String> workerIds, String details) {

    /**
     * How far from a chaos event a successful handling may lie to be blamed on it. Generous: the
     * finalize that a kill or an outage interrupts normally follows the handler by milliseconds.
     */
    public static final Duration ATTRIBUTION_WINDOW = Duration.ofSeconds(10);

    public ChaosEvent {
        Objects.requireNonNull(kind, "kind must not be null");
        Objects.requireNonNull(at, "at must not be null");
        workerIds = List.copyOf(Objects.requireNonNullElse(workerIds, List.of()));
        details = Objects.requireNonNullElse(details, "");
    }

    /**
     * Whether a successful handling by {@code workerId} that finished at {@code finishedAt} could
     * have lost its finalize to this event.
     */
    public boolean explains(String workerId, Instant finishedAt) {
        boolean inWindow =
                !finishedAt.isBefore(at.minus(ATTRIBUTION_WINDOW))
                        && !finishedAt.isAfter(at.plus(ATTRIBUTION_WINDOW));
        if (!inWindow) {
            return false;
        }
        return switch (kind) {
            case WORKER_KILLED -> workerIds.contains(workerId);
            case POSTGRES_RESTARTED -> true;
            case WORKER_SPAWNED -> false;
        };
    }

    /** The kinds of action the harness can take. */
    public enum Kind {
        WORKER_KILLED,
        WORKER_SPAWNED,
        POSTGRES_RESTARTED
    }
}
