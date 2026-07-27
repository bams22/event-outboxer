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
 *   <li>{@link #put(OutboxMetricsSnapshot)} replaces any existing entry unconditionally; TTL
 *       semantics are owned by the implementation.
 *   <li>{@link #invalidate()} drops the cached entry immediately. Used by tests and by operator
 *       tooling that needs the next read to be fresh.
 *   <li>Implementations MUST be thread-safe.
 * </ul>
 *
 * <h2>Built-in flavours</h2>
 *
 * <ul>
 *   <li>{@link #noop()} — disables caching entirely. Every {@link #get()} misses and every {@link
 *       #put(OutboxMetricsSnapshot)} is dropped; adapters compute on each call.
 *   <li>{@link #inMemory(Clock, Duration)} — per-JVM {@code AtomicReference} with TTL. Matches the
 *       pre-SPI behaviour of {@code PostgresEventStore}; suitable for single-pod deployments or
 *       when cross-pod snapshot consistency does not matter.
 * </ul>
 *
 * <p>For cross-pod consistency see the {@code event-outboxer-cache-redis} module, which ships a
 * {@code StatefulRedisConnection}-backed implementation.
 */
public interface MetricsSnapshotCache {

  /** Returns the cached snapshot if present and not expired. */
  Optional<OutboxMetricsSnapshot> get();

  /** Stores {@code snapshot}; callers pass a freshly-computed aggregate. */
  void put(OutboxMetricsSnapshot snapshot);

  /** Drops the cached entry. Next {@link #get()} misses. */
  void invalidate();

  /** Cache that never stores anything — every {@link #get()} misses. */
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
