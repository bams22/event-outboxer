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
 * @param claimMinFree free in-flight capacity ({@code handlerPoolSize + handlerQueueCapacity -
 *     inFlight}) the poller waits for before it claims again — a low-watermark refill. {@code 1}
 *     (the default) claims as soon as a single slot frees; a larger value lets slots accumulate so
 *     one claim statement refills the executor in bulk instead of one row per handler completion.
 *     Must be in {@code [1, handlerPoolSize + handlerQueueCapacity]} (validated). With a platform
 *     executor a value above {@code handlerQueueCapacity} idles handler threads while the poller
 *     waits for the threshold — keep it at or below the queue size there; for the virtual-thread
 *     executor it deliberately trades in-flight concurrency for batching
 * @param handlerPoolSize size of the per-type handler executor thread pool
 * @param handlerQueueCapacity bounded queue size for the handler executor; zero means a synchronous
 *     handoff that fails fast (triggers {@code onDispatchRejected})
 * @param handlerMaxRuntime maximum handler wall-clock time before the watchdog force-reclaims the
 *     row
 * @param interruptStuckHandler whether the watchdog interrupts the dispatching thread after a
 *     force-reclaim. On by default: the force-reclaim already invalidated the handler's finalize
 *     (ADR-0014), so a handler left running can only waste a slot of this type's pool. Turn it off
 *     for handlers that are not interrupt-safe — the row is reclaimed either way, but the thread is
 *     then never asked to stop
 * @param lockTtl safety timeout passed to {@code EntityLocker.tryLock(...)}. Must be {@code >=
 *     handlerMaxRuntime} (validated): for TTL-honouring lockers (Redis) a shorter TTL would let the
 *     entity lock expire while a legitimate handler is still running, breaking per-key
 *     serialization. Recommended {@code >= 2 x handlerMaxRuntime} — the TTL is the crash-release
 *     mechanism, and the margin covers a zombie handler that outlives its force-reclaimed claim
 * @param lockWait how long the dispatcher keeps retrying a busy entity lock on the handler thread
 *     before it gives up and releases the event with {@code lockBusyRetryDelay} (ADR-0035). Default
 *     100 ms, set by the benchmark session of 2026-09-04: it removes practically every busy round
 *     trip for handlers in the millisecond range and gives up quickly on slow holders. Zero means
 *     one non-blocking attempt — the pre-ADR-0035 behaviour. Must be {@code < handlerMaxRuntime}
 *     (validated): the wait runs inside the in-flight window and spends the watchdog's budget
 */
@Builder(toBuilder = true)
public record EventTypeConfig(
        Duration pollMinInterval,
        Duration pollMaxInterval,
        double pollMultiplier,
        int claimBatchSize,
        int claimMinFree,
        int handlerPoolSize,
        int handlerQueueCapacity,
        Duration handlerMaxRuntime,
        boolean interruptStuckHandler,
        Duration lockTtl,
        Duration lockWait) {

    public EventTypeConfig {
        Objects.requireNonNull(pollMinInterval, "pollMinInterval must not be null");
        Objects.requireNonNull(pollMaxInterval, "pollMaxInterval must not be null");
        Objects.requireNonNull(handlerMaxRuntime, "handlerMaxRuntime must not be null");
        Objects.requireNonNull(lockTtl, "lockTtl must not be null");
        Objects.requireNonNull(lockWait, "lockWait must not be null");
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
        if (claimMinFree < 1) {
            throw new IllegalArgumentException("claimMinFree must be >= 1, got " + claimMinFree);
        }
        if (claimMinFree > handlerPoolSize + handlerQueueCapacity) {
            throw new IllegalArgumentException(
                    "claimMinFree ("
                            + claimMinFree
                            + ") must not exceed handlerPoolSize + handlerQueueCapacity ("
                            + (handlerPoolSize + handlerQueueCapacity)
                            + "): the poller would wait for more free capacity than the executor"
                            + " can ever have and never claim again.");
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
        if (lockWait.isNegative()) {
            throw new IllegalArgumentException("lockWait must not be negative, got " + lockWait);
        }
        if (lockWait.compareTo(handlerMaxRuntime) >= 0) {
            throw new IllegalArgumentException(
                    "lockWait ("
                            + lockWait
                            + ") must be < handlerMaxRuntime ("
                            + handlerMaxRuntime
                            + "): the bounded lock wait runs inside the in-flight window and spends"
                            + " the watchdog's budget; a wait as long as the budget would let the"
                            + " watchdog reclaim events that never reached their handler.");
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
                .claimMinFree(1)
                .handlerPoolSize(3)
                .handlerQueueCapacity(100)
                .handlerMaxRuntime(Duration.ofMinutes(5))
                .interruptStuckHandler(true)
                // 2 x handlerMaxRuntime: the TTL is the crash-release mechanism, and the margin
                // covers
                // a zombie handler finishing after its row was already force-reclaimed by the
                // watchdog.
                .lockTtl(Duration.ofMinutes(10))
                // ADR-0035: measured on the hot-key, mixed and crash presets (2026-09-04 session);
                // 100 ms turned 0.8 busy round trips per event into none on 5 ms holds and gave up
                // fast enough on a 200 ms holder. Zero restores the one-attempt flow of ADR-0012.
                .lockWait(Duration.ofMillis(100))
                .build();
    }
}
