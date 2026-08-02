/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.cache.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.bams22.outboxer.spi.MetricsSnapshotCache;
import io.github.bams22.outboxer.spi.OutboxMetricsSnapshot;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link MetricsSnapshotCache} backed by Redis / KeyDB. One cache key per deployment; every pod
 * reads and writes the same key so {@code /actuator/health/outbox} returns a consistent snapshot
 * across the fleet regardless of which replica serves the probe.
 *
 * <h2>Storage shape</h2>
 *
 * <ul>
 *   <li>Key = {@code <keyPrefix>snapshot} (default prefix {@code outbox:metrics:}).
 *   <li>Value = Jackson-JSON serialisation of {@link OutboxMetricsSnapshot}.
 *   <li>TTL enforced server-side via {@code SET key value PX <millis>} — atomic with the write.
 * </ul>
 *
 * <h2>Error semantics (fail-safe)</h2>
 *
 * Any Redis failure or JSON-deserialisation error on {@link #get()} is logged and swallowed,
 * returning {@link Optional#empty()}. Callers (typically {@code PostgresEventStore}) interpret this
 * as a cache miss and fall back to recomputing the snapshot from the database. This keeps the
 * health endpoint functional during a Redis outage — operators lose the shared-view guarantee but
 * do not lose the health probe itself.
 *
 * <h2>Threading</h2>
 *
 * Safe for concurrent use — delegates to Lettuce's sync {@link RedisCommands}, which is
 * thread-safe.
 *
 * <h2>Lifecycle</h2>
 *
 * The cache does NOT own its {@link StatefulRedisConnection}. The connection (and its backing
 * {@code RedisClient}) are created, configured and closed by the caller — usually the Spring Boot
 * starter.
 */
public final class LettuceMetricsSnapshotCache implements MetricsSnapshotCache {

    private static final Logger log = LoggerFactory.getLogger(LettuceMetricsSnapshotCache.class);

    /** Default key prefix — aligns with the other Redis adapters in the project. */
    public static final String DEFAULT_KEY_PREFIX = "outbox:metrics:";

    private static final String KEY_SUFFIX = "snapshot";

    private final RedisCommands<String, String> commands;
    private final Duration ttl;
    private final String key;
    private final ObjectMapper mapper;

    /**
     * Convenience constructor using the default key prefix and a Jackson mapper tuned for {@code
     * java.time} records (the {@code takenAt} / {@code oldestPendingRunAt} fields of {@link
     * OutboxMetricsSnapshot}).
     */
    public LettuceMetricsSnapshotCache(
            StatefulRedisConnection<String, String> connection, Duration ttl) {
        this(connection, ttl, DEFAULT_KEY_PREFIX, defaultMapper());
    }

    public LettuceMetricsSnapshotCache(
            StatefulRedisConnection<String, String> connection,
            Duration ttl,
            String keyPrefix,
            ObjectMapper mapper) {
        Objects.requireNonNull(connection, "connection must not be null");
        this.ttl = Objects.requireNonNull(ttl, "ttl must not be null");
        if (ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("ttl must be positive, got " + ttl);
        }
        Objects.requireNonNull(keyPrefix, "keyPrefix must not be null");
        this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
        this.commands = connection.sync();
        this.key = keyPrefix + KEY_SUFFIX;
    }

    @Override
    public Optional<OutboxMetricsSnapshot> get() {
        String raw;
        try {
            raw = commands.get(key);
        } catch (RuntimeException ex) {
            log.warn("redis GET failed for metrics cache key '{}': {}", key, ex.toString());
            return Optional.empty();
        }
        if (raw == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(mapper.readValue(raw, OutboxMetricsSnapshot.class));
        } catch (RuntimeException | JsonProcessingException ex) {
            log.warn(
                    "failed to deserialise cached metrics snapshot (treating as miss): {}",
                    ex.toString());
            return Optional.empty();
        }
    }

    @Override
    public void put(OutboxMetricsSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        String payload;
        try {
            payload = mapper.writeValueAsString(snapshot);
        } catch (JsonProcessingException ex) {
            log.warn("failed to serialise metrics snapshot for Redis cache: {}", ex.toString());
            return;
        }
        try {
            commands.set(key, payload, SetArgs.Builder.px(ttl.toMillis()));
        } catch (RuntimeException ex) {
            log.warn("redis SET failed for metrics cache key '{}': {}", key, ex.toString());
        }
    }

    @Override
    public void invalidate() {
        try {
            commands.del(key);
        } catch (RuntimeException ex) {
            log.warn("redis DEL failed for metrics cache key '{}': {}", key, ex.toString());
        }
    }

    private static ObjectMapper defaultMapper() {
        return JsonMapper.builder().addModule(new JavaTimeModule()).build();
    }
}
