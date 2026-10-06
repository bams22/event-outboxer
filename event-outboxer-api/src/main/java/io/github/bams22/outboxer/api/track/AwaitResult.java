/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.api.track;

import io.github.bams22.outboxer.domain.ArchivedEvent;
import io.github.bams22.outboxer.domain.Event;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Outcome of {@link OutboxEventTracker#await(UUID, Duration)}. Sealed — exactly three results are
 * possible: {@link Completed}, {@link Disabled} and {@link TimedOut}. The records stay public for
 * pattern matching ({@code switch (result) { case Disabled d -> ... }}).
 */
public sealed interface AwaitResult
        permits AwaitResult.Completed, AwaitResult.Disabled, AwaitResult.TimedOut {

    /**
     * Id of the awaited event.
     */
    UUID eventId();

    /**
     * {@code true} for {@link Completed}.
     */
    default boolean isCompleted() {
        return this instanceof Completed;
    }

    /**
     * {@code true} for {@link Disabled}.
     */
    default boolean isDisabled() {
        return this instanceof Disabled;
    }

    /**
     * {@code true} for {@link TimedOut}.
     */
    default boolean isTimedOut() {
        return this instanceof TimedOut;
    }

    /**
     * The event left the hot table: the outbox finalised it after at least one {@code Success} or
     * {@code Skip} (ADR-0015 — the handler may have run more than once).
     *
     * <p>An id whose publishing transaction rolled back is also "not in the hot table" and is
     * reported here with {@code archived == null}: {@code await} is the handle for ids the caller
     * committed (ADR-0038 §4).
     *
     * @param eventId id of the awaited event
     * @param archived the archive row when the archive is enabled and holds one; {@code null} when
     *     the archive is disabled, unavailable, or has no row for this id
     */
    record Completed(UUID eventId, @Nullable ArchivedEvent archived) implements AwaitResult {

        public Completed {
            Objects.requireNonNull(eventId, "eventId must not be null");
        }
    }

    /**
     * The event reached {@code DISABLED}: it will not be processed again unless an operator
     * re-enables it.
     *
     * @param event the hot-table row as last read, carrying {@code attempts()} and {@code
     *     lastFailReason()}
     */
    record Disabled(Event event) implements AwaitResult {

        public Disabled {
            Objects.requireNonNull(event, "event must not be null");
        }

        @Override
        public UUID eventId() {
            return event.id();
        }
    }

    /**
     * The deadline passed while the event was still in the hot table and not disabled — "not yet",
     * as opposed to "broken".
     *
     * @param eventId id of the awaited event
     * @param lastSeen state observed by the last lookup before the deadline ({@link
     *     TrackedState#PENDING} or {@link TrackedState#PROCESSING})
     * @param waited wall-clock time spent waiting; never negative
     */
    record TimedOut(UUID eventId, TrackedState lastSeen, Duration waited) implements AwaitResult {

        public TimedOut {
            Objects.requireNonNull(eventId, "eventId must not be null");
            Objects.requireNonNull(lastSeen, "lastSeen must not be null");
            Objects.requireNonNull(waited, "waited must not be null");
            if (waited.isNegative()) {
                throw new IllegalArgumentException("waited must not be negative, got " + waited);
            }
        }
    }
}
