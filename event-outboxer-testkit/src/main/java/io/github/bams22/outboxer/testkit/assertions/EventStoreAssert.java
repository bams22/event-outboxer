/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.testkit.assertions;

import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.OutboxMetricsSnapshot;
import java.util.Optional;
import java.util.UUID;
import org.assertj.core.api.AbstractAssert;

/**
 * AssertJ-style fluent assertions over an {@link EventStore}. Typically obtained via {@link
 * EventAssertions#assertThatStore(EventStore)}.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * assertThatStore(store)
 *     .hasEvent(eventId)
 *     .withStatus(EventStatus.PROCESSING)
 *     .withAttempts(0);
 * assertThatStore(store).hasNoEvent(otherId);
 * assertThatStore(store).hasTotalPending(3);
 * }</pre>
 */
public final class EventStoreAssert extends AbstractAssert<EventStoreAssert, EventStore> {

    public EventStoreAssert(EventStore actual) {
        super(actual, EventStoreAssert.class);
    }

    /**
     * Assert that the store contains an event with the given id and expose it for further checks.
     */
    public EventAssert hasEvent(UUID id) {
        isNotNull();
        Optional<Event> found = actual.findById(id);
        if (found.isEmpty()) {
            failWithMessage("Expected EventStore to contain event <%s> but it was not found.", id);
        }
        return new EventAssert(found.orElseThrow());
    }

    /** Assert that the store does NOT contain an event with the given id. */
    public EventStoreAssert hasNoEvent(UUID id) {
        isNotNull();
        if (actual.findById(id).isPresent()) {
            failWithMessage(
                    "Expected EventStore NOT to contain event <%s> but it was present.", id);
        }
        return this;
    }

    /** Assert the count of PENDING rows across all types. */
    public EventStoreAssert hasTotalPending(long expected) {
        isNotNull();
        OutboxMetricsSnapshot snapshot = actual.metricsSnapshot();
        if (snapshot.totalPending() != expected) {
            failWithMessage(
                    "Expected total PENDING events <%s> but was <%s>.",
                    expected, snapshot.totalPending());
        }
        return this;
    }

    /** Assert the count of PROCESSING rows across all types. */
    public EventStoreAssert hasTotalProcessing(long expected) {
        isNotNull();
        OutboxMetricsSnapshot snapshot = actual.metricsSnapshot();
        if (snapshot.totalProcessing() != expected) {
            failWithMessage(
                    "Expected total PROCESSING events <%s> but was <%s>.",
                    expected, snapshot.totalProcessing());
        }
        return this;
    }

    /** Assert the count of DISABLED rows across all types. */
    public EventStoreAssert hasTotalDisabled(long expected) {
        isNotNull();
        OutboxMetricsSnapshot snapshot = actual.metricsSnapshot();
        if (snapshot.totalDisabled() != expected) {
            failWithMessage(
                    "Expected total DISABLED events <%s> but was <%s>.",
                    expected, snapshot.totalDisabled());
        }
        return this;
    }

    // -----------------------------------------------------------------------------------------

    /** Fluent check over a single {@link Event}. Returned by {@link #hasEvent(UUID)}. */
    public static final class EventAssert extends AbstractAssert<EventAssert, Event> {

        private EventAssert(Event actual) {
            super(actual, EventAssert.class);
        }

        public EventAssert withStatus(EventStatus expected) {
            if (actual.status() != expected) {
                failWithMessage(
                        "Expected event <%s> to have status <%s> but was <%s>.",
                        actual.id(), expected, actual.status());
            }
            return this;
        }

        public EventAssert withAttempts(int expected) {
            if (actual.attempts() != expected) {
                failWithMessage(
                        "Expected event <%s> to have attempts <%s> but was <%s>.",
                        actual.id(), expected, actual.attempts());
            }
            return this;
        }

        public EventAssert withEventType(String expected) {
            if (!actual.eventType().equals(expected)) {
                failWithMessage(
                        "Expected event <%s> to have eventType <%s> but was <%s>.",
                        actual.id(), expected, actual.eventType());
            }
            return this;
        }

        public EventAssert withLastFailReason(String expected) {
            if (actual.lastFailReason() == null || !actual.lastFailReason().equals(expected)) {
                failWithMessage(
                        "Expected event <%s> to have lastFailReason <%s> but was <%s>.",
                        actual.id(), expected, actual.lastFailReason());
            }
            return this;
        }

        public EventAssert withVersionGreaterThan(long floor) {
            if (actual.version() <= floor) {
                failWithMessage(
                        "Expected event <%s> to have version greater than <%s> but was <%s>.",
                        actual.id(), floor, actual.version());
            }
            return this;
        }

        /** Get the underlying {@link Event} for ad-hoc extra assertions. */
        public Event actual() {
            return actual;
        }
    }
}
