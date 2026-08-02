/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.maintenance;

import io.github.bams22.outboxer.core.concurrent.NamedThreadFactory;
import io.github.bams22.outboxer.core.config.MaintenanceConfig;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.Nullable;

/**
 * Owns a small scheduled executor (three threads by default) that ticks {@link HeartbeatTask},
 * {@link OrphanRecoveryTask}, {@link WatchdogTask} and {@link EngineHealthCheckTask}. The executor
 * is created on {@link #start()} and shut down on {@link #stop(Duration)} so the engine's
 * graceful-shutdown flow can drain pending work deterministically.
 */
public final class MaintenanceScheduler {

    private final HeartbeatTask heartbeat;
    private final OrphanRecoveryTask orphanRecovery;
    private final WatchdogTask watchdog;
    private final EngineHealthCheckTask engineHealthCheck;
    private final @Nullable RetentionTask retention;
    private final StaleClaimSweeperTask staleClaimSweeper;
    private final MaintenanceConfig config;

    private @Nullable ScheduledExecutorService executor;

    public MaintenanceScheduler(
            HeartbeatTask heartbeat,
            OrphanRecoveryTask orphanRecovery,
            WatchdogTask watchdog,
            EngineHealthCheckTask engineHealthCheck,
            @Nullable RetentionTask retention,
            StaleClaimSweeperTask staleClaimSweeper,
            MaintenanceConfig config) {
        this.heartbeat = Objects.requireNonNull(heartbeat);
        this.orphanRecovery = Objects.requireNonNull(orphanRecovery);
        this.watchdog = Objects.requireNonNull(watchdog);
        this.engineHealthCheck = Objects.requireNonNull(engineHealthCheck);
        this.retention = retention;
        this.staleClaimSweeper = Objects.requireNonNull(staleClaimSweeper);
        this.config = Objects.requireNonNull(config);
    }

    /** Start ticking. Must be called exactly once. */
    public synchronized void start() {
        if (executor != null) {
            throw new IllegalStateException("maintenance scheduler already started");
        }
        executor =
                Executors.newScheduledThreadPool(
                        3, new NamedThreadFactory("outbox-maintenance", true));
        scheduleFixed(executor, heartbeat, config.heartbeatInterval());
        scheduleFixed(executor, orphanRecovery, config.orphanRecoveryInterval());
        scheduleFixed(executor, watchdog, config.watchdogInterval());
        // Crash detection ticks at the same cadence as the watchdog — no new config knob.
        scheduleFixed(executor, engineHealthCheck, config.watchdogInterval());
        if (retention != null) {
            scheduleFixed(executor, retention, retention.interval());
        }
        scheduleFixed(executor, staleClaimSweeper, staleClaimSweeper.interval());
    }

    /**
     * Request shutdown and wait up to {@code timeout} for in-flight tasks to drain. Callers are
     * expected to have already stopped the pollers so the watchdog has stable state to snapshot.
     */
    public synchronized void stop(Duration timeout) {
        ScheduledExecutorService exec = executor;
        if (exec == null) {
            return;
        }
        exec.shutdown();
        try {
            if (!exec.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                exec.shutdownNow();
            }
        } catch (InterruptedException ie) {
            exec.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            executor = null;
        }
    }

    private static void scheduleFixed(
            ScheduledExecutorService exec, Runnable task, Duration interval) {
        long nanos = Math.max(1_000_000L, interval.toNanos());
        exec.scheduleWithFixedDelay(task, nanos, nanos, TimeUnit.NANOSECONDS);
    }
}
