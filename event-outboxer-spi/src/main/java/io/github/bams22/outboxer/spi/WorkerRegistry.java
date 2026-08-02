/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spi;

import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.domain.WorkerInfo;
import io.github.bams22.outboxer.domain.exception.WorkerRegistryException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Registry of live worker JVMs that are currently serving the outbox. Backed by the {@code
 * event_outboxer.workers} table in the PostgreSQL adapter (see ADR-0005 and STORAGE.md §Tables).
 *
 * <p>Having the heartbeat live in a dedicated table — rather than on every in-flight event row —
 * keeps heartbeat writes O(1) per JVM regardless of concurrent event load. Orphan recovery uses
 * {@link #findDead(Duration, int)} to identify workers whose heartbeat is stale and then reclaims
 * all events owned by those workers in a single pass (see ADR-0005 and ADR-0014).
 *
 * <p>All methods are thread-safe and may be invoked concurrently by the engine's {@code
 * HeartbeatTask} and {@code OrphanRecoveryTask}.
 */
public interface WorkerRegistry {

    /**
     * Register a new worker. Called once at engine startup. If a row with the same {@code WorkerId}
     * already exists (for example because the previous JVM crashed without deregistering), the
     * adapter must overwrite it and reset the heartbeat to the current clock.
     *
     * @throws WorkerRegistryException if the registry cannot persist the worker
     */
    void register(WorkerInfo info);

    /**
     * Update the heartbeat timestamp for the given worker to the supplied {@code at}. Called
     * periodically by the engine's {@code HeartbeatTask}. The adapter returns {@code false} if no
     * row exists for the given id (for example, the worker was already reaped by another instance's
     * orphan-recovery pass).
     *
     * @return {@code true} if the heartbeat row was updated
     * @throws WorkerRegistryException if the registry cannot perform the update
     */
    boolean heartbeat(WorkerId id, Instant at);

    /**
     * Mark a worker as gracefully shutting down. Used as a hint for other JVMs to prioritize orphan
     * reclaim of this worker's events without waiting for the dead threshold. Implementations that
     * do not support this hint may treat it as a no-op.
     *
     * @throws WorkerRegistryException if the registry cannot perform the update
     */
    void markGracefulStop(WorkerId id);

    /**
     * Remove a worker from the registry. Called at the end of a clean shutdown after in-flight
     * handlers have completed.
     *
     * @throws WorkerRegistryException if the registry cannot perform the deletion
     */
    void deregister(WorkerId id);

    /**
     * Return workers whose most recent heartbeat is older than {@code now - deadThreshold}. Capped
     * by {@code limit} so that a single orphan-recovery pass over a catastrophically stale cluster
     * does not take unbounded time.
     *
     * @param deadThreshold minimum age of the last heartbeat before a worker is considered dead
     * @param limit maximum number of workers to return in one call; must be positive
     * @throws WorkerRegistryException if the registry cannot be queried
     */
    List<WorkerInfo> findDead(Duration deadThreshold, int limit);

    /**
     * Hard-delete the given dead workers from the registry. Called by the orphan-recovery flow
     * after all events owned by these workers have been reclaimed to {@code PENDING}.
     *
     * @throws WorkerRegistryException if the registry cannot perform the deletion
     */
    void removeDead(List<WorkerId> ids);

    /**
     * Look up a single worker by id. Returns {@link Optional#empty()} if no row exists.
     *
     * @throws WorkerRegistryException if the registry cannot be queried
     */
    Optional<WorkerInfo> findById(WorkerId id);

    /**
     * Return every currently registered worker. Intended for diagnostics / admin views rather than
     * hot-path orphan recovery, so no batch-size cap is imposed by the SPI.
     *
     * @throws WorkerRegistryException if the registry cannot be queried
     */
    List<WorkerInfo> findAll();
}
