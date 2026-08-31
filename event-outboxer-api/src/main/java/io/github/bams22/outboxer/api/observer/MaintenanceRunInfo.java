/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.api.observer;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Payload of {@link OutboxListener#onMaintenanceRunCompleted(MaintenanceRunInfo)} — fired after
 * every run of a periodic maintenance task, successful or not. A {@code FAILED} run is not fatal:
 * the scheduler catches the failure and the task runs again at its next cadence; sustained failures
 * of the same task are the signal to alert on.
 *
 * <p>{@code task} is one of a small, stable set of names: {@code heartbeat}, {@code
 * orphan_recovery}, {@code watchdog}, {@code engine_health_check}, {@code retention}, {@code
 * stale_claim_sweeper} — safe to use as a metric tag.
 *
 * @param task stable name of the maintenance task
 * @param result whether the run completed normally or threw
 * @param cause the failure when {@code result == FAILED}; {@code null} on {@code OK}
 */
public record MaintenanceRunInfo(String task, Result result, @Nullable Throwable cause) {

    public MaintenanceRunInfo {
        Objects.requireNonNull(task, "task must not be null");
        if (task.isBlank()) {
            throw new IllegalArgumentException("task must not be blank");
        }
        Objects.requireNonNull(result, "result must not be null");
    }

    /** Outcome of a single maintenance task run. */
    public enum Result {
        OK,
        FAILED
    }
}
