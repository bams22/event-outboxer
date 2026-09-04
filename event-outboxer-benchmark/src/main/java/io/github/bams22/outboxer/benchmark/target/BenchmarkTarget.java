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

import org.jspecify.annotations.Nullable;

/**
 * A system under test. The harness ships one implementation, the event-outboxer target; the seam
 * exists so a team can measure another outbox with the same scenarios from an adapter kept in its
 * own repository (ADR-0034 §2).
 */
public interface BenchmarkTarget {

    /** Short name that appears in the report and the console summary. */
    String name();

    /**
     * Database schema the target keeps its state in. Its row writes are attributed to the target
     * and it must be clean (no event rows, no lock rows) once a session is closed.
     */
    String storageSchema();

    /**
     * Qualified name of the table that holds queued events: vacuumed before a run, sized after the
     * publish phase, and expected to be empty after the graceful stop.
     */
    String eventsTable();

    /**
     * Qualified name of the table that holds lease rows with an {@code expires_at} column and an
     * {@code owner_worker} column, or {@code null} when the target keeps no such table. Live rows
     * left in it after the stop fail the storage-cleanliness invariant.
     */
    @Nullable String leaseTable();

    /**
     * Boots the publisher side and returns a session; workers start on {@link
     * TargetSession#startWorkers()}. The target is responsible for migrating its own schema.
     */
    TargetSession open(BenchmarkEnvironment environment);
}
