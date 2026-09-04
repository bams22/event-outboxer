/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.scenario;

import lombok.Builder;

/**
 * What goes wrong on purpose during the drain (ADR-0034 phase 2). Every action fires once, when the
 * share of handled events reaches its trigger; the triggers are evaluated only after the publish
 * phase, so a steady-state scenario whose fleet outruns the publisher may never reach a trigger —
 * use backlog mode ({@code workersStartAfterPublish}) for chaos runs.
 *
 * @param killWorkers workers to {@code SIGKILL}; {@code 0} = none. Requires the forked fleet.
 * @param killAtProgress handled share in {@code [0, 1)} at which the kill fires
 * @param respawnKilled {@code true} = boot a replacement worker (fresh id) for each killed one, as
 *     an orchestrator would; {@code false} = the survivors must absorb the load
 * @param postgresRestart how PostgreSQL is restarted; {@link PostgresRestart#NONE} = not at all
 * @param postgresRestartAtProgress handled share in {@code [0, 1)} at which the restart fires
 */
@Builder(toBuilder = true)
public record Chaos(
        int killWorkers,
        double killAtProgress,
        boolean respawnKilled,
        PostgresRestart postgresRestart,
        double postgresRestartAtProgress) {

    public Chaos {
        if (killWorkers < 0) {
            throw new IllegalArgumentException("killWorkers must be >= 0, got " + killWorkers);
        }
        share("killAtProgress", killAtProgress);
        postgresRestart = postgresRestart == null ? PostgresRestart.NONE : postgresRestart;
        share("postgresRestartAtProgress", postgresRestartAtProgress);
    }

    /** Nothing goes wrong. */
    public static Chaos none() {
        return new Chaos(0, 0.0, true, PostgresRestart.NONE, 0.0);
    }

    /** Whether any action is configured. */
    public boolean any() {
        return killWorkers > 0 || postgresRestart != PostgresRestart.NONE;
    }

    private static void share(String field, double value) {
        if (value < 0.0 || value >= 1.0) {
            throw new IllegalArgumentException(field + " must be within [0, 1), got " + value);
        }
    }
}
