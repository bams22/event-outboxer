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
import java.util.Objects;
import java.util.Optional;

/**
 * Cache for {@link OutboxMetricsSnapshot} produced by {@link EventStore#metricsSnapshot()}.
 * Adapters call {@link #get()} before running the relatively expensive aggregate query and {@link
 * #put(OutboxMetricsSnapshot)} to populate it on a miss.
 *
 * <h2>Why it is a port</h2>
 *
 * Health-indicator scrapes across a fleet of pods read independent local caches by default, so each
 * pod reports a snapshot taken within the last TTL — values routinely disagree across replicas
 * which makes operator dashboards look like flapping. A shared cache (Redis, Caffeine with a
 * distributed invalidation bus, Hazelcast, …) collapses all pods onto one snapshot per TTL window
 * and is the motivation for making this pluggable.
 *
 * <h2>Contract</h2>
 *
 * <ul>
 *   <li>{@link #get()} returns {@link Optional#empty()} on a miss, expired entry, or any
 *       deserialisation / backend error (fail-safe: adapters must then fall back to computing the
 *       snapshot from scratch).
 *   <li>{@link #put(OutboxMetricsSnapshot)} replaces any existing entry, <em>unless</em> the
 *       snapshot was taken before the last {@link #invalidate()} — see "Invalidation" below. TTL
 *       semantics are owned by the implementation.
 *   <li>{@link #invalidate()} drops the cached entry immediately and bars any snapshot taken
 *       before it from being stored afterwards.
 *   <li>Implementations MUST be thread-safe.
 *   <li>No method may propagate a backend failure to its caller. {@link #get()} degrades to a
 *       miss; {@link #put(OutboxMetricsSnapshot)} and {@link #invalidate()} log and return. A
 *       cache that throws would turn a successful database mutation into a reported failure — the
 *       adapters guard against it, but a custom implementation must not rely on that.
 * </ul>
 *
 * <h2>Invalidation</h2>
 *
 * The PostgreSQL {@code OutboxAdmin} calls {@link #invalidate()} after every mutation that changed
 * rows (re-enable, purge of {@code DISABLED} rows, replay), so the store's next {@code
 * metricsSnapshot()} reflects it instead of serving pre-mutation counts until the TTL runs out.
 * Tests and operator tooling call it for the same reason.
 *
 * <p>Dropping the entry is not enough on its own: a snapshot computation that started before the
 * mutation can finish after the invalidation and {@link #put(OutboxMetricsSnapshot)} its
 * pre-mutation counts back, which would restore exactly the staleness the invalidation removed.
 * That is why {@link #put(OutboxMetricsSnapshot)} is conditional — an implementation remembers when
 * it was last invalidated and ignores a snapshot whose {@link OutboxMetricsSnapshot#takenAt()} is
 * before that moment. {@code takenAt} is captured before the aggregate query runs, so "taken
 * before the invalidation" means "cannot be trusted to include the mutation". The cost of a
 * dropped {@code put} is one recomputation, never a wrong number.
 *
 * <p>Two limits are worth knowing. The comparison assumes the cache and the {@code EventStore}
 * read the same wall clock — the built-in flavours and the starter's wiring do; a custom cache on
 * a different {@link Clock} weakens it, and the Redis flavour compares timestamps written by
 * different pods, so it is only as good as their clock sync. And an admin mutation that runs
 * inside a caller transaction becomes visible at commit, not when its statement ran, so the
 * invalidation has to be deferred to the commit — the Spring Boot starter does this for the admin
 * bean it wires; plain-Java wiring that calls the admin inside its own transaction should invoke
 * {@link #invalidate()} after committing.
 *
 * <h2>Built-in flavours</h2>
 *
 * <ul>
 *   <li>{@link #noop()} — disables caching entirely. Every {@link #get()} misses and every {@link
 *       #put(OutboxMetricsSnapshot)} is dropped; adapters compute on each call.
 *   <li>{@link #inMemory(Clock, Duration)} — per-JVM {@code AtomicReference} with TTL. Matches the
 *       pre-SPI behaviour of {@code PostgresEventStore}; suitable for single-pod deployments or
 *       when cross-pod snapshot consistency does not matter. Being per-JVM, an invalidation
 *       reaches only the replica that performed the admin mutation.
 * </ul>
 *
 * <p>For cross-pod consistency see the {@code event-outboxer-cache-redis} module, which ships a
 * {@code StatefulRedisConnection}-backed implementation.
 */
public interface MetricsSnapshotCache {

    /**
     * Returns the cached snapshot if present and not expired.
     */
    Optional<OutboxMetricsSnapshot> get();

    /**
     * Stores {@code snapshot}, unless it was taken before the last {@link #invalidate()} — a
     * snapshot that old cannot be trusted to include the mutation that invalidated the cache, so it
     * is dropped rather than cached. Callers pass a freshly-computed aggregate.
     */
    void put(OutboxMetricsSnapshot snapshot);

    /**
     * Drops the cached entry: the next {@link #get()} misses, and any snapshot taken before this
     * call is refused by {@link #put(OutboxMetricsSnapshot)}.
     */
    void invalidate();

    /**
     * Cache that never stores anything — every {@link #get()} misses.
     */
    static MetricsSnapshotCache noop() {
        return NoopMetricsSnapshotCache.INSTANCE;
    }

    /**
     * Per-JVM TTL cache backed by a single {@code AtomicReference}. {@code clock} drives expiry
     * decisions so tests can feed a {@code SettableClock}; production code passes {@link
     * Clock#system()}.
     */
    static MetricsSnapshotCache inMemory(Clock clock, Duration ttl) {
        Objects.requireNonNull(clock, "clock must not be null");
        Objects.requireNonNull(ttl, "ttl must not be null");
        if (ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("ttl must be positive, got " + ttl);
        }
        return new InMemoryMetricsSnapshotCache(clock, ttl);
    }
}
