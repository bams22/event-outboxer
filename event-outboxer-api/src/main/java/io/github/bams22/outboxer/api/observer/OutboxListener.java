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

/**
 * Observability event bus for the outbox engine. Implementations may be used to publish metrics
 * (the {@code event-outboxer-metrics-micrometer} module ships a Micrometer-backed implementation),
 * write structured logs, maintain an audit trail, trigger alerts, or power distributed-tracing
 * integrations.
 *
 * <p>Every method has a default no-op implementation so that custom listeners only have to override
 * the events they care about.
 *
 * <h2>Threading contract</h2>
 *
 * Callbacks are invoked from a variety of threads — worker threads processing events, poller
 * threads, and the shared maintenance executor. Implementations MUST be thread-safe. Callbacks also
 * run on the engine's hot path, so they MUST be fast and non-blocking: any long-running work should
 * be offloaded to a dedicated executor owned by the listener.
 *
 * <h2>Failure isolation</h2>
 *
 * The engine wraps each listener invocation in a try/catch so that a misbehaving listener cannot
 * take other listeners or the engine down. Exceptions are logged; they are not propagated to the
 * event's processing flow.
 */
public interface OutboxListener {

    /**
     * Listener whose every callback is the interface's default no-op. Exists so that wiring classes
     * have a non-null default collaborator (mirrors {@code EntityLocker.NOOP} and {@code
     * OutboxTracer.NOOP}); use it instead of an ad-hoc {@code new OutboxListener() {}}.
     */
    OutboxListener NOOP = new OutboxListener() {};

    // ==================== Publication ====================

    /**
     * Called after {@code OutboxEventPublisher.publish(...)} (or {@code publishAll}) persisted a
     * {@code PendingEvent}. Note: the caller's transaction may still roll back, in which case the
     * event is never visible to the engine.
     */
    default void onEventPublished(EventPublishedInfo info) {
        // no-op
    }

    /**
     * Called when a publish carrying a dedup key coalesced into an existing {@code PENDING} event
     * of the same {@code (type, key)} instead of inserting a new row (ADR-0021). Fires instead of
     * {@code onEventPublished} for that request. Note: the caller's transaction may still roll
     * back, in which case the coalescing never becomes visible to the engine.
     */
    default void onEventCoalesced(EventCoalescedInfo info) {
        // no-op
    }

    // ==================== Polling ====================

    /**
     * Called after every claim attempt of a per-type poller, including empty polls. This is the
     * highest-frequency callback in the interface — up to once per {@code pollMinInterval} (default
     * 500 ms) per event type — so implementations must be strictly O(1) and allocation-light.
     */
    default void onPollCompleted(PollCompletedInfo info) {
        // no-op
    }

    /**
     * Called when a per-type poller skipped a claim cycle because the handler executor had no free
     * capacity. Sustained firing means the type's pool/queue budget is undersized for its load.
     */
    default void onPollerSaturated(PollerSaturatedInfo info) {
        // no-op
    }

    // ==================== Processing lifecycle ====================

    /** Called when a worker successfully claimed an event and is about to dispatch it. */
    default void onEventClaimed(EventClaimedInfo info) {
        // no-op
    }

    /**
     * Called after a handler returned {@code EventOutcome.Success} and the storage acknowledged the
     * finalization.
     */
    default void onEventProcessed(EventProcessedInfo info) {
        // no-op
    }

    /** Called when the failure chain re-scheduled the event and the storage acknowledged it. */
    default void onEventRetryScheduled(EventRetryScheduledInfo info) {
        // no-op
    }

    /** Called when the failure chain moved the event to {@code DISABLED}. */
    default void onEventDisabled(EventDisabledInfo info) {
        // no-op
    }

    /** Called when the failure chain deleted the event outright. */
    default void onEventDeleted(EventDeletedInfo info) {
        // no-op
    }

    /** Called when a handler returned {@code EventOutcome.Skip}. */
    default void onEventSkipped(EventSkippedInfo info) {
        // no-op
    }

    // ==================== Errors & anomalies ====================

    /** Called when a handler threw an exception (before the failure chain runs). */
    default void onHandlerError(HandlerErrorInfo info) {
        // no-op
    }

    /**
     * Called when a claimed event has no registered handler for its type; the {@code
     * UnknownHandlerPolicy} decides what happens next.
     */
    default void onUnknownEventType(UnknownEventTypeInfo info) {
        // no-op
    }

    /**
     * Called when the engine could not deserialize the payload of a claimed event — including the
     * case where no serializer is registered for the event's stored payload format (ADR-0025).
     */
    default void onEventSerializationError(SerializationErrorInfo info) {
        // no-op
    }

    /**
     * Called when {@code EntityLocker.tryLock(...)} yielded the lock for a handler that declares a
     * lock key, immediately before the handler runs. Fires for every acquisition; {@code
     * info.waited()} separates the immediate ones from those that needed the bounded wait of
     * ADR-0035.
     */
    default void onLockAcquired(LockAcquiredInfo info) {
        // no-op
    }

    /**
     * Called when {@code EntityLocker.tryLock(...)} returned empty because the key is held by
     * another worker — after the type's bounded {@code lockWait} elapsed, if one is configured
     * (ADR-0035). This is the normal busy-lock path, not a technical failure.
     */
    default void onLockAcquisitionFailed(LockAcquisitionInfo info) {
        // no-op
    }

    /**
     * Called when {@code LockHandle.close()} swallowed a release error. The lock will free itself
     * when its TTL expires or the session ends.
     */
    default void onLockReleaseFailed(LockReleaseInfo info) {
        // no-op
    }

    // ==================== Worker lifecycle ====================

    /** Called once at engine startup after the worker row is inserted. */
    default void onWorkerRegistered(WorkerRegisteredInfo info) {
        // no-op
    }

    /** Called when the worker flagged itself as shutting down gracefully. */
    default void onWorkerGracefulStop(WorkerGracefulStopInfo info) {
        // no-op
    }

    /** Called at the end of graceful shutdown, after the worker row has been removed. */
    default void onWorkerDeregistered(WorkerDeregisteredInfo info) {
        // no-op
    }

    /** Called when the periodic heartbeat failed to update the worker row in storage. */
    default void onHeartbeatFailed(HeartbeatFailedInfo info) {
        // no-op
    }

    // ==================== Recovery ====================

    /** Called after the orphan-recovery task returned dead workers' events to PENDING. */
    default void onOrphansReclaimed(OrphansReclaimedInfo info) {
        // no-op
    }

    /**
     * Called when the watchdog force-reclaimed an event whose handler had exceeded {@code
     * handlerMaxRuntime}.
     */
    default void onStuckHandlerReclaimed(StuckHandlerReclaimedInfo info) {
        // no-op
    }

    /**
     * Called once per force-reclaimed dispatch that was still running after {@code
     * abandonedHandlerGrace} — its thread is lost to the type's handler pool until it returns on
     * its own. {@code info.interrupted()} distinguishes a handler that ignored the interrupt from a
     * type that opted out of being interrupted at all.
     */
    default void onHandlerAbandoned(HandlerAbandonedInfo info) {
        // no-op
    }

    // ==================== Maintenance ====================

    /**
     * Called when the stale-claim sweeper released events back to {@code PENDING} whose claims
     * outlived the threshold without a live in-flight registration. Fires only when at least one
     * claim was swept.
     */
    default void onStaleClaimsSwept(StaleClaimsSweptInfo info) {
        // no-op
    }

    /**
     * Called when the retention task permanently deleted rows past their retention window. Fires
     * only when at least one row was purged.
     */
    default void onRetentionPurged(RetentionPurgedInfo info) {
        // no-op
    }

    /**
     * Called after every run of a periodic maintenance task, successful or failed. A failed run is
     * caught by the scheduler and the task simply runs again at its next cadence; {@code
     * info.task()} is one of a small stable set of names, safe to use as a metric tag.
     */
    default void onMaintenanceRunCompleted(MaintenanceRunInfo info) {
        // no-op
    }

    // ==================== Storage ====================

    /** Called when any storage operation raised a {@code StorageException}. */
    default void onStorageError(StorageErrorInfo info) {
        // no-op
    }

    // ==================== Dispatch / backpressure ====================

    /** Called when the per-type handler executor rejected the dispatch of a claimed event. */
    default void onDispatchRejected(DispatchRejectedInfo info) {
        // no-op
    }

    // ==================== Engine crash ====================

    /**
     * Called when the engine's background health check detected that a critical component is no
     * longer alive — typically, a per-type poller thread died from an uncaught {@code Error}. After
     * this callback {@code OutboxEngine.state()} reports {@code STOPPED}; the Actuator health
     * indicator flips DOWN, the {@code event_outboxer.engine.state} gauge flips to {@code
     * stopped=1}, and (with {@code event-outboxer.health.probe-groups} configured) the readiness /
     * liveness probe follows.
     */
    default void onEngineCrashed(EngineCrashedInfo info) {
        // no-op
    }
}
