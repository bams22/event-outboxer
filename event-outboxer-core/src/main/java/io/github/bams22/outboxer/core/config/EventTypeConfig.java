/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.config;

import java.time.Duration;
import java.util.Objects;
import lombok.Builder;

/**
 * Per-event-type runtime configuration. One instance drives the poller, handler executor and
 * watchdog for a given event type (see ADR-0004).
 *
 * <p>Defaults mirror CONFIGURATION.md §per-type and are applied via {@link #defaults()}; explicit
 * overrides go through the Lombok-generated builder.
 *
 * @param pollMinInterval minimum wait between claim attempts when the store is busy; also the
 *     baseline {@link io.github.bams22.outboxer.core.polling.AdaptiveWaiter}
 * @param pollMaxInterval cap on the adaptive wait when the store is idle
 * @param pollMultiplier per-empty-batch multiplier applied to the current wait; must be {@code >
 *     1.0}
 * @param claimBatchSize maximum number of events claimed per poll
 * @param handlerPoolSize size of the per-type handler executor thread pool
 * @param handlerQueueCapacity bounded queue size for the handler executor; zero means a synchronous
 *     handoff that fails fast (triggers {@code onDispatchRejected})
 * @param handlerMaxRuntime maximum handler wall-clock time before the watchdog force-reclaims the
 *     row
 * @param lockTtl safety timeout passed to {@code EntityLocker.tryLock(...)}. Must be {@code >=
 *     handlerMaxRuntime} (validated): for TTL-honouring lockers (Redis) a shorter TTL would let the
 *     entity lock expire while a legitimate handler is still running, breaking per-key
 *     serialization. Recommended {@code >= 2 x handlerMaxRuntime} — the TTL is the crash-release
 *     mechanism, and the margin covers a zombie handler that outlives its force-reclaimed claim
 */
@Builder(toBuilder = true)
public record EventTypeConfig(
        Duration pollMinInterval,
        Duration pollMaxInterval,
        double pollMultiplier,
        int claimBatchSize,
        int handlerPoolSize,
        int handlerQueueCapacity,
        Duration handlerMaxRuntime,
        Duration lockTtl) {

    public EventTypeConfig {
        Objects.requireNonNull(pollMinInterval, "pollMinInterval must not be null");
        Objects.requireNonNull(pollMaxInterval, "pollMaxInterval must not be null");
        Objects.requireNonNull(handlerMaxRuntime, "handlerMaxRuntime must not be null");
        Objects.requireNonNull(lockTtl, "lockTtl must not be null");
        if (pollMinInterval.isNegative() || pollMinInterval.isZero()) {
            throw new IllegalArgumentException(
                    "pollMinInterval must be positive, got " + pollMinInterval);
        }
        if (pollMaxInterval.compareTo(pollMinInterval) < 0) {
            throw new IllegalArgumentException(
                    "pollMaxInterval must be >= pollMinInterval, got min="
                            + pollMinInterval
                            + ", max="
                            + pollMaxInterval);
        }
        if (pollMultiplier <= 1.0) {
            throw new IllegalArgumentException(
                    "pollMultiplier must be > 1.0, got " + pollMultiplier);
        }
        if (claimBatchSize <= 0) {
            throw new IllegalArgumentException(
                    "claimBatchSize must be positive, got " + claimBatchSize);
        }
        if (handlerPoolSize <= 0) {
            throw new IllegalArgumentException(
                    "handlerPoolSize must be positive, got " + handlerPoolSize);
        }
        if (handlerQueueCapacity < 0) {
            throw new IllegalArgumentException(
                    "handlerQueueCapacity must not be negative, got " + handlerQueueCapacity);
        }
        if (handlerMaxRuntime.isNegative() || handlerMaxRuntime.isZero()) {
            throw new IllegalArgumentException(
                    "handlerMaxRuntime must be positive, got " + handlerMaxRuntime);
        }
        if (lockTtl.isNegative() || lockTtl.isZero()) {
            throw new IllegalArgumentException("lockTtl must be positive, got " + lockTtl);
        }
        if (lockTtl.compareTo(handlerMaxRuntime) < 0) {
            throw new IllegalArgumentException(
                    "lockTtl ("
                            + lockTtl
                            + ") must be >= handlerMaxRuntime ("
                            + handlerMaxRuntime
                            + "): a TTL shorter than the handler budget lets the entity lock expire"
                            + " while a legitimate handler is still running, breaking per-key"
                            + " serialization for TTL-honouring lockers (Redis). Recommended:"
                            + " lockTtl >= 2 x handlerMaxRuntime.");
        }
    }

    /**
     * Default configuration aligned with CONFIGURATION.md §per-type. Conservative values suitable
     * for most workloads; tune per-type in production.
     */
    public static EventTypeConfig defaults() {
        return EventTypeConfig.builder()
                .pollMinInterval(Duration.ofMillis(500))
                .pollMaxInterval(Duration.ofSeconds(10))
                .pollMultiplier(1.5)
                .claimBatchSize(10)
                .handlerPoolSize(3)
                .handlerQueueCapacity(100)
                .handlerMaxRuntime(Duration.ofMinutes(5))
                // 2 x handlerMaxRuntime: the TTL is the crash-release mechanism, and the margin
                // covers
                // a zombie handler finishing after its row was already force-reclaimed by the
                // watchdog.
                .lockTtl(Duration.ofMinutes(10))
                .build();
    }
}
