/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.track;

import io.github.bams22.outboxer.api.track.AwaitResult;
import io.github.bams22.outboxer.api.track.OutboxEventTracker;
import io.github.bams22.outboxer.api.track.TrackedState;
import io.github.bams22.outboxer.core.publish.TransactionContext;
import io.github.bams22.outboxer.domain.ArchivedEvent;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.exception.AwaitInTransactionException;
import io.github.bams22.outboxer.domain.exception.AwaitInterruptedException;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.OutboxAdmin;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import lombok.Builder;
import org.jspecify.annotations.Nullable;

/**
 * Default {@link OutboxEventTracker} (ADR-0038). Holds no per-call state and no engine lifecycle —
 * a single instance serves every thread, on full and publish-only instances alike. Every answer
 * comes from the database: {@link EventStore#findById(UUID)} for the hot table and, when an admin
 * port is configured, {@link OutboxAdmin#findInArchive(UUID)} for the archive.
 *
 * <p>{@code await} polls by primary key on the calling thread with {@link Thread#sleep(Duration)}
 * against {@link System#nanoTime()} — the wait is wall-clock by nature, so no {@code Clock} port is
 * involved.
 *
 * <p><b>Construction.</b> {@code DefaultOutboxEventTracker.builder()}. Required: {@code store}.
 * Defaulted: {@code admin} ({@code null} — the archive is never consulted, a missing row maps to
 * {@link TrackedState#ABSENT} / {@code Completed(archived = null)}), {@code transactionContext}
 * ({@link TransactionContext#neverActive()}) and {@code pollInterval} ({@link
 * #DEFAULT_POLL_INTERVAL}).
 *
 * <p><b>Transaction-context default.</b> The tracker defaults to {@code neverActive()} while
 * {@code DefaultOutboxEventPublisher} defaults to {@code alwaysActive()}. The asymmetry is
 * deliberate: each default is the one that does not break the common plain-Java case for its
 * component — the publisher must not refuse to write, the tracker must not refuse to wait, when no
 * transaction manager is observable. With a real context (the Spring Boot starter), {@code await}
 * inside a transaction fails fast with {@link AwaitInTransactionException}.
 */
public final class DefaultOutboxEventTracker implements OutboxEventTracker {

    /**
     * Default pause between two lookups of {@code await}: 200 ms.
     */
    public static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMillis(200);

    private final EventStore store;
    private final @Nullable OutboxAdmin admin;
    private final TransactionContext transactionContext;
    private final Duration pollInterval;

    /**
     * Builder-backed constructor; parameter names are the builder's method names.
     *
     * @param store hot-table lookups
     * @param admin archive lookups; {@code null} = the archive is never consulted
     * @param transactionContext detects an active transaction on the calling thread; {@code null}
     *     = {@link TransactionContext#neverActive()} (guard off, see the class Javadoc)
     * @param pollInterval default pause between lookups of {@link #await(UUID, Duration)}; {@code
     *     null} = {@link #DEFAULT_POLL_INTERVAL}; must be positive
     */
    @Builder
    private DefaultOutboxEventTracker(
            EventStore store,
            @Nullable OutboxAdmin admin,
            @Nullable TransactionContext transactionContext,
            @Nullable Duration pollInterval) {
        this.store = Objects.requireNonNull(store, "store must not be null");
        this.admin = admin;
        this.transactionContext =
                transactionContext != null ? transactionContext : TransactionContext.neverActive();
        this.pollInterval =
                requirePositive(
                        pollInterval != null ? pollInterval : DEFAULT_POLL_INTERVAL,
                        "pollInterval");
    }

    @Override
    public TrackedState state(UUID eventId) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Optional<Event> row = store.findById(eventId);
        if (row.isPresent()) {
            return toTrackedState(row.get().status());
        }
        return findInArchive(eventId) != null ? TrackedState.ARCHIVED : TrackedState.ABSENT;
    }

    @Override
    public AwaitResult await(UUID eventId, Duration timeout) {
        return await(eventId, timeout, pollInterval);
    }

    @Override
    public AwaitResult await(UUID eventId, Duration timeout, Duration pollInterval) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        requirePositive(timeout, "timeout");
        requirePositive(pollInterval, "pollInterval");
        if (transactionContext.isActive()) {
            throw new AwaitInTransactionException(
                    "OutboxEventTracker.await("
                            + eventId
                            + ") called inside an active"
                            + " transaction; the event becomes visible to the engine only after"
                            + " that transaction commits — call await after the publishing"
                            + " transaction commits");
        }
        long start = System.nanoTime();
        long deadline = start + timeout.toNanos();
        while (true) {
            Optional<Event> row = store.findById(eventId);
            if (row.isEmpty()) {
                return new AwaitResult.Completed(eventId, findInArchive(eventId));
            }
            Event event = row.get();
            if (event.status() == EventStatus.DISABLED) {
                return new AwaitResult.Disabled(event);
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return new AwaitResult.TimedOut(
                        eventId,
                        toTrackedState(event.status()),
                        Duration.ofNanos(System.nanoTime() - start));
            }
            sleep(Math.min(pollInterval.toNanos(), remaining), eventId);
        }
    }

    private @Nullable ArchivedEvent findInArchive(UUID eventId) {
        return admin == null ? null : admin.findInArchive(eventId).orElse(null);
    }

    private static TrackedState toTrackedState(EventStatus status) {
        return switch (status) {
            case PENDING -> TrackedState.PENDING;
            case PROCESSING -> TrackedState.PROCESSING;
            case DISABLED -> TrackedState.DISABLED;
        };
    }

    private static void sleep(long nanos, UUID eventId) {
        try {
            Thread.sleep(Duration.ofNanos(nanos));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AwaitInterruptedException("interrupted while awaiting event " + eventId, e);
        }
    }

    private static Duration requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isNegative() || value.isZero()) {
            throw new IllegalArgumentException(name + " must be positive, got " + value);
        }
        return value;
    }
}
