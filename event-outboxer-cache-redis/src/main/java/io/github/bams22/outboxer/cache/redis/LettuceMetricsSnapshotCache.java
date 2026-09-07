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
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.MetricsSnapshotCache;
import io.github.bams22.outboxer.spi.OutboxMetricsSnapshot;
import io.lettuce.core.ScriptOutputType;
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
 *   <li>Key = {@code <keyPrefix>invalidated-at} — epoch millis of the last {@link #invalidate()},
 *       the barrier described below. Written with a one-hour expiry, far longer than any snapshot
 *       computation, and refreshed on every invalidation.
 * </ul>
 *
 * <h2>Invalidation barrier</h2>
 *
 * {@link MetricsSnapshotCache} requires a {@link #put(OutboxMetricsSnapshot)} to be refused when
 * the snapshot was taken before the last {@link #invalidate()}: otherwise a scrape whose aggregate
 * query started before an admin mutation could store its pre-mutation counts right after the
 * invalidation dropped them. Both operations therefore run as Lua scripts so the read of the
 * barrier and the write of the snapshot are one atomic step on the server.
 *
 * <p>Across a fleet the barrier is written by the pod that ran the admin mutation and compared
 * against a {@code takenAt} stamped by whichever pod is scraping, at millisecond resolution. It is
 * therefore only as precise as the clock sync between them — with NTP-disciplined hosts the
 * residual window is sub-millisecond, and losing it costs at most one TTL of staleness, never a
 * wrong number. The per-JVM {@code MetricsSnapshotCache.inMemory} flavour has one clock and no such
 * window.
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

    /**
     * Default key prefix — aligns with the other Redis adapters in the project.
     */
    public static final String DEFAULT_KEY_PREFIX = "outbox:metrics:";

    private static final String KEY_SUFFIX = "snapshot";
    private static final String INVALIDATED_AT_SUFFIX = "invalidated-at";

    /**
     * Expiry of the invalidation barrier. It only has to outlive an in-flight snapshot computation
     * — an aggregate over the outbox tables, milliseconds to seconds — so an hour is generous, and
     * it keeps the key from outliving a deployment that stops using this cache.
     */
    private static final Duration INVALIDATED_AT_TTL = Duration.ofHours(1);

    /**
     * Stores the snapshot unless the barrier is younger than it. KEYS = snapshot key, barrier key;
     * ARGV = payload, {@code takenAt} epoch millis, snapshot TTL in millis.
     */
    private static final String PUT_SCRIPT =
            "local mark = redis.call('get', KEYS[2]) "
                    + "if mark and tonumber(mark) > tonumber(ARGV[2]) then "
                    + "  return 0 "
                    + "end "
                    + "redis.call('set', KEYS[1], ARGV[1], 'PX', ARGV[3]) "
                    + "return 1";

    /**
     * Drops the snapshot and moves the barrier forward, never backward — a delayed invalidation
     * from a pod whose clock lags must not lower a barrier another pod already raised. KEYS =
     * snapshot key, barrier key; ARGV = now in epoch millis, barrier TTL in millis.
     */
    private static final String INVALIDATE_SCRIPT =
            "local mark = redis.call('get', KEYS[2]) "
                    + "local at = ARGV[1] "
                    + "if mark and tonumber(mark) > tonumber(at) then "
                    + "  at = mark "
                    + "end "
                    + "redis.call('set', KEYS[2], at, 'PX', ARGV[2]) "
                    + "redis.call('del', KEYS[1]) "
                    + "return 1";

    private final RedisCommands<String, String> commands;
    private final Clock clock;
    private final Duration ttl;
    private final String key;
    private final String invalidatedAtKey;
    private final ObjectMapper mapper;

    /**
     * Convenience constructor using the default key prefix and a Jackson mapper tuned for {@code
     * java.time} records (the {@code takenAt} / {@code oldestPendingRunAt} fields of {@link
     * OutboxMetricsSnapshot}).
     */
    public LettuceMetricsSnapshotCache(
            StatefulRedisConnection<String, String> connection, Duration ttl, Clock clock) {
        this(connection, ttl, DEFAULT_KEY_PREFIX, defaultMapper(), clock);
    }

    /**
     * @param clock stamps the invalidation barrier. Pass the same {@code Clock} the {@code
     *     EventStore} uses to stamp {@code takenAt}, otherwise the two are compared across
     *     different time bases — the starter wires the application's outbox {@code Clock} bean here
     */
    public LettuceMetricsSnapshotCache(
            StatefulRedisConnection<String, String> connection,
            Duration ttl,
            String keyPrefix,
            ObjectMapper mapper,
            Clock clock) {
        Objects.requireNonNull(connection, "connection must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.ttl = Objects.requireNonNull(ttl, "ttl must not be null");
        if (ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("ttl must be positive, got " + ttl);
        }
        Objects.requireNonNull(keyPrefix, "keyPrefix must not be null");
        this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
        this.commands = connection.sync();
        this.key = keyPrefix + KEY_SUFFIX;
        this.invalidatedAtKey = keyPrefix + INVALIDATED_AT_SUFFIX;
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
        Long stored;
        try {
            stored =
                    commands.eval(
                            PUT_SCRIPT,
                            ScriptOutputType.INTEGER,
                            new String[] {key, invalidatedAtKey},
                            payload,
                            Long.toString(snapshot.takenAt().toEpochMilli()),
                            Long.toString(ttl.toMillis()));
        } catch (RuntimeException ex) {
            log.warn("redis SET failed for metrics cache key '{}': {}", key, ex.toString());
            return;
        }
        if (stored != null && stored == 0L) {
            log.debug(
                    "metrics snapshot taken at {} dropped: the cache was invalidated after it was"
                            + " computed",
                    snapshot.takenAt());
        }
    }

    @Override
    public void invalidate() {
        try {
            commands.eval(
                    INVALIDATE_SCRIPT,
                    ScriptOutputType.INTEGER,
                    new String[] {key, invalidatedAtKey},
                    Long.toString(clock.now().toEpochMilli()),
                    Long.toString(INVALIDATED_AT_TTL.toMillis()));
        } catch (RuntimeException ex) {
            log.warn("redis DEL failed for metrics cache key '{}': {}", key, ex.toString());
        }
    }

    private static ObjectMapper defaultMapper() {
        return JsonMapper.builder().addModule(new JavaTimeModule()).build();
    }
}
