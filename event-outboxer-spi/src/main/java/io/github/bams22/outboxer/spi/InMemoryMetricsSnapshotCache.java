/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spi;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;

/**
 * Per-JVM TTL {@link MetricsSnapshotCache} backed by a single {@link AtomicReference}. Replaces the
 * ad-hoc cache that used to live inside {@code PostgresEventStore}. Exposed through the {@link
 * MetricsSnapshotCache#inMemory(Clock, Duration)} factory.
 *
 * <p>A second reference holds the moment of the last {@link #invalidate()}. A snapshot taken before
 * it is refused by {@link #put(OutboxMetricsSnapshot)}, so a computation that was already running
 * when an admin mutation invalidated the cache cannot put its pre-mutation counts back — see the
 * "Invalidation" section of {@link MetricsSnapshotCache}. Both references are read and written
 * without locking; {@code put} stores first and withdraws its own entry if the barrier moved
 * underneath it, which is what makes the check-then-act pair safe.
 */
final class InMemoryMetricsSnapshotCache implements MetricsSnapshotCache {

    private final Clock clock;
    private final Duration ttl;
    private final AtomicReference<@Nullable Entry> ref = new AtomicReference<>();
    private final AtomicReference<@Nullable Instant> invalidatedAt = new AtomicReference<>();

    InMemoryMetricsSnapshotCache(Clock clock, Duration ttl) {
        this.clock = clock;
        this.ttl = ttl;
    }

    @Override
    public Optional<OutboxMetricsSnapshot> get() {
        Entry entry = ref.get();
        if (entry == null) {
            return Optional.empty();
        }
        if (clock.now().isAfter(entry.takenAt.plus(ttl))) {
            return Optional.empty();
        }
        return Optional.of(entry.snapshot);
    }

    @Override
    public void put(OutboxMetricsSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        if (staleFor(invalidatedAt.get(), snapshot)) {
            return;
        }
        Entry entry = new Entry(snapshot, clock.now());
        ref.set(entry);
        if (staleFor(invalidatedAt.get(), snapshot)) {
            // An invalidation landed between the check above and the store: withdraw our own
            // entry, and only ours — a fresher one may already have replaced it.
            ref.compareAndSet(entry, null);
        }
    }

    @Override
    public void invalidate() {
        Instant now = clock.now();
        invalidatedAt.updateAndGet(prev -> prev == null || now.isAfter(prev) ? now : prev);
        ref.set(null);
    }

    private static boolean staleFor(@Nullable Instant barrier, OutboxMetricsSnapshot snapshot) {
        return barrier != null && snapshot.takenAt().isBefore(barrier);
    }

    private record Entry(OutboxMetricsSnapshot snapshot, Instant takenAt) {}
}
