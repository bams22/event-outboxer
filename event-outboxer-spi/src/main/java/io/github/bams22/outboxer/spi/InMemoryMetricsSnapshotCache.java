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
 */
final class InMemoryMetricsSnapshotCache implements MetricsSnapshotCache {

    private final Clock clock;
    private final Duration ttl;
    private final AtomicReference<@Nullable Entry> ref = new AtomicReference<>();

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
        ref.set(new Entry(snapshot, clock.now()));
    }

    @Override
    public void invalidate() {
        ref.set(null);
    }

    private record Entry(OutboxMetricsSnapshot snapshot, Instant takenAt) {}
}
