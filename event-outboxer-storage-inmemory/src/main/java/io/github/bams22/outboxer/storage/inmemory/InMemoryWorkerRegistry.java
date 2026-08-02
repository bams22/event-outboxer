/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.storage.inmemory;

import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.domain.WorkerInfo;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.WorkerRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-memory {@link WorkerRegistry} backed by a {@link ConcurrentHashMap}. Heartbeat timestamps and
 * graceful-stop flags are stored next to the immutable {@link WorkerInfo}.
 */
public final class InMemoryWorkerRegistry implements WorkerRegistry {

    private final ConcurrentMap<WorkerId, Entry> entries = new ConcurrentHashMap<>();
    private final Clock clock;

    public InMemoryWorkerRegistry() {
        this(Clock.system());
    }

    public InMemoryWorkerRegistry(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public void register(WorkerInfo info) {
        Objects.requireNonNull(info, "info must not be null");
        entries.put(info.id(), new Entry(info, clock.now(), false));
    }

    @Override
    public boolean heartbeat(WorkerId id, Instant at) {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(at, "at must not be null");
        Entry updated = entries.computeIfPresent(id, (k, cur) -> cur.withHeartbeat(at));
        return updated != null;
    }

    @Override
    public void markGracefulStop(WorkerId id) {
        Objects.requireNonNull(id, "id must not be null");
        entries.computeIfPresent(id, (k, cur) -> cur.withGracefulStop());
    }

    @Override
    public void deregister(WorkerId id) {
        Objects.requireNonNull(id, "id must not be null");
        entries.remove(id);
    }

    @Override
    public List<WorkerInfo> findDead(Duration deadThreshold, int limit) {
        Objects.requireNonNull(deadThreshold, "deadThreshold must not be null");
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive, got " + limit);
        }
        Instant cutoff = clock.now().minus(deadThreshold);
        // graceful_stop counts as immediately dead — the SPI documents the flag as a hint to
        // reclaim without waiting for the dead threshold (mirrors the PostgreSQL adapter).
        return entries.values().stream()
                .filter(e -> e.gracefulStop || e.lastHeartbeat.isBefore(cutoff))
                .sorted(Comparator.comparing(e -> e.lastHeartbeat))
                .limit(limit)
                .map(Entry::info)
                .toList();
    }

    @Override
    public void removeDead(List<WorkerId> ids) {
        Objects.requireNonNull(ids, "ids must not be null");
        for (WorkerId id : ids) {
            entries.remove(id);
        }
    }

    @Override
    public Optional<WorkerInfo> findById(WorkerId id) {
        Objects.requireNonNull(id, "id must not be null");
        Entry e = entries.get(id);
        return e == null ? Optional.empty() : Optional.of(e.info);
    }

    @Override
    public List<WorkerInfo> findAll() {
        List<WorkerInfo> out = new ArrayList<>(entries.size());
        for (Entry e : entries.values()) {
            out.add(e.info);
        }
        return List.copyOf(out);
    }

    private record Entry(WorkerInfo info, Instant lastHeartbeat, boolean gracefulStop) {

        Entry withHeartbeat(Instant at) {
            return new Entry(info, at, gracefulStop);
        }

        Entry withGracefulStop() {
            return new Entry(info, lastHeartbeat, true);
        }
    }
}
