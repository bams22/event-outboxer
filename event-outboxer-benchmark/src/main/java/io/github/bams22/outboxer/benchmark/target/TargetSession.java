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

import java.util.List;

/**
 * A running target: publisher up, workers started on demand. {@link #close()} must stop the fleet
 * gracefully — the storage-cleanliness invariant is graded after it returns, so a session that
 * abandons in-flight claims fails the run.
 */
public interface TargetSession extends AutoCloseable {

    /** The publisher for this session; safe to call from several threads. */
    BenchmarkPublisher publisher();

    /**
     * Starts the worker fleet. Called once per session, either before publishing (steady state) or
     * after it (backlog mode), as the scenario dictates.
     */
    void startWorkers();

    /** Number of workers currently running. */
    int workerCount();

    /** Ids of the workers currently running, oldest first. */
    List<String> workerIds();

    /**
     * {@code SIGKILL}s {@code count} running workers (oldest first) and returns their ids. No
     * graceful stop, no claim release: what a node loss looks like.
     *
     * @throws UnsupportedOperationException when the fleet cannot be killed honestly (in-process)
     */
    List<String> killWorkers(int count);

    /**
     * Boots {@code count} additional workers with fresh ids, as an orchestrator replacing lost pods
     * would, and returns their ids once they are ready.
     */
    List<String> spawnWorkers(int count);

    @Override
    void close();
}
