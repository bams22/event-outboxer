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

import io.github.bams22.outboxer.api.observer.MaintenanceRunInfo;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.core.concurrent.NamedThreadFactory;
import io.github.bams22.outboxer.core.config.MaintenanceConfig;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Builder;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns a small scheduled executor (three threads by default) that ticks {@link HeartbeatTask},
 * {@link OrphanRecoveryTask}, {@link WatchdogTask} and {@link EngineHealthCheckTask}. The executor
 * is created on {@link #start()} and shut down on {@link #stop(Duration)} so the engine's
 * graceful-shutdown flow can drain pending work deterministically.
 *
 * <p>Every task run is guarded: a {@code RuntimeException} is caught here (so {@code
 * scheduleWithFixedDelay} keeps rescheduling the task — a propagated exception would cancel it
 * silently and forever), logged, and reported through {@code
 * OutboxListener.onMaintenanceRunCompleted(...)} together with successful runs. An {@code Error}
 * still cancels that one task's schedule — deliberate: the JVM is in an undefined state.
 *
 * <p><b>Construction.</b> {@code MaintenanceScheduler.builder()} — see the constructor for required
 * collaborators and defaults.
 */
public final class MaintenanceScheduler {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceScheduler.class);

    private final HeartbeatTask heartbeat;
    private final OrphanRecoveryTask orphanRecovery;
    private final WatchdogTask watchdog;
    private final EngineHealthCheckTask engineHealthCheck;
    private final @Nullable RetentionTask retention;
    private final @Nullable StaleClaimSweeperTask staleClaimSweeper;
    private final MaintenanceConfig config;
    private final OutboxListener listener;

    private @Nullable ScheduledExecutorService executor;

    /**
     * Builder-backed constructor; parameter names are the builder's method names. Required: {@code
     * heartbeat}, {@code orphanRecovery}, {@code watchdog}, {@code engineHealthCheck}. {@code
     * retention} and {@code staleClaimSweeper} are genuinely optional — {@code null} means the task
     * is disabled (retention off; no sweeper on an instance that polls no type and has no explicit
     * threshold); {@code config} defaults to {@link MaintenanceConfig#defaults()}; {@code listener}
     * defaults to {@link OutboxListener#NOOP}.
     */
    @Builder
    private MaintenanceScheduler(
            HeartbeatTask heartbeat,
            OrphanRecoveryTask orphanRecovery,
            WatchdogTask watchdog,
            EngineHealthCheckTask engineHealthCheck,
            @Nullable RetentionTask retention,
            @Nullable StaleClaimSweeperTask staleClaimSweeper,
            @Nullable MaintenanceConfig config,
            @Nullable OutboxListener listener) {
        this.heartbeat = Objects.requireNonNull(heartbeat, "heartbeat must not be null");
        this.orphanRecovery =
                Objects.requireNonNull(orphanRecovery, "orphanRecovery must not be null");
        this.watchdog = Objects.requireNonNull(watchdog, "watchdog must not be null");
        this.engineHealthCheck =
                Objects.requireNonNull(engineHealthCheck, "engineHealthCheck must not be null");
        this.retention = retention;
        this.staleClaimSweeper = staleClaimSweeper;
        this.config = config != null ? config : MaintenanceConfig.defaults();
        this.listener = listener != null ? listener : OutboxListener.NOOP;
    }

    /** Start ticking. Must be called exactly once. */
    public synchronized void start() {
        if (executor != null) {
            throw new IllegalStateException("maintenance scheduler already started");
        }
        executor =
                Executors.newScheduledThreadPool(
                        3, new NamedThreadFactory("outbox-maintenance", true));
        scheduleFixed(executor, "heartbeat", heartbeat, config.heartbeatInterval());
        scheduleFixed(executor, "orphan_recovery", orphanRecovery, config.orphanRecoveryInterval());
        scheduleFixed(executor, "watchdog", watchdog, config.watchdogInterval());
        // Crash detection ticks at the same cadence as the watchdog — no new config knob.
        scheduleFixed(
                executor, "engine_health_check", engineHealthCheck, config.watchdogInterval());
        if (retention != null) {
            scheduleFixed(executor, "retention", retention, retention.interval());
        }
        if (staleClaimSweeper != null) {
            scheduleFixed(
                    executor,
                    "stale_claim_sweeper",
                    staleClaimSweeper,
                    staleClaimSweeper.interval());
        }
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

    private void scheduleFixed(
            ScheduledExecutorService exec, String name, Runnable task, Duration interval) {
        long nanos = Math.max(1_000_000L, interval.toNanos());
        exec.scheduleWithFixedDelay(
                () -> runGuarded(name, task), nanos, nanos, TimeUnit.NANOSECONDS);
    }

    /**
     * The single catch-and-continue barrier for maintenance work. The listener call is guarded
     * separately: a throwing listener must neither turn an OK run into a FAILED one nor cancel the
     * task's schedule.
     */
    private void runGuarded(String name, Runnable task) {
        MaintenanceRunInfo info;
        try {
            task.run();
            info = new MaintenanceRunInfo(name, MaintenanceRunInfo.Result.OK, null);
        } catch (RuntimeException ex) {
            log.warn("maintenance task {} failed; will retry next pass: {}", name, ex.toString());
            info = new MaintenanceRunInfo(name, MaintenanceRunInfo.Result.FAILED, ex);
        }
        try {
            listener.onMaintenanceRunCompleted(info);
        } catch (RuntimeException ex) {
            log.warn(
                    "listener threw on maintenance run completion of task {}: {}",
                    name,
                    ex.toString());
        }
    }
}
