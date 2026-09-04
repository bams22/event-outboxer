/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.target;

import io.github.bams22.outboxer.benchmark.db.DatabaseCoordinates;
import io.github.bams22.outboxer.benchmark.ledger.Ledger;
import io.github.bams22.outboxer.benchmark.scenario.Scenario;
import java.nio.file.Path;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * What the harness hands a target when opening a session: where the database is, what the run looks
 * like, and the ledger every handling must be written to.
 *
 * @param database JDBC coordinates of the benchmark database (migrated by the target itself)
 * @param redisUri Lettuce URI of the Redis the {@code redis} locker uses; {@code null} otherwise
 * @param scenario the effective scenario
 * @param ledger the ledger the target's handlers report into (in-process) or the driver reads
 *     (forked: workers open their own writer onto the same table)
 * @param workDir scratch directory for the run: worker spec files, ready markers, worker logs
 */
public record BenchmarkEnvironment(
        DatabaseCoordinates database,
        @Nullable String redisUri,
        Scenario scenario,
        Ledger ledger,
        Path workDir) {

    public BenchmarkEnvironment {
        Objects.requireNonNull(database, "database must not be null");
        Objects.requireNonNull(scenario, "scenario must not be null");
        Objects.requireNonNull(ledger, "ledger must not be null");
        Objects.requireNonNull(workDir, "workDir must not be null");
    }
}
