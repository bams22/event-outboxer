/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.metrics.micrometer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.api.observer.EventClaimedInfo;
import io.github.bams22.outboxer.api.observer.EventCoalescedInfo;
import io.github.bams22.outboxer.api.observer.EventDeletedInfo;
import io.github.bams22.outboxer.api.observer.EventDisabledInfo;
import io.github.bams22.outboxer.api.observer.EventProcessedInfo;
import io.github.bams22.outboxer.api.observer.EventPublishedInfo;
import io.github.bams22.outboxer.api.observer.EventRetryScheduledInfo;
import io.github.bams22.outboxer.api.observer.HandlerAbandonedInfo;
import io.github.bams22.outboxer.api.observer.HandlerErrorInfo;
import io.github.bams22.outboxer.api.observer.HeartbeatFailedInfo;
import io.github.bams22.outboxer.api.observer.LockAcquiredInfo;
import io.github.bams22.outboxer.api.observer.LockAcquisitionInfo;
import io.github.bams22.outboxer.api.observer.LockReleasedInfo;
import io.github.bams22.outboxer.api.observer.MaintenanceRunInfo;
import io.github.bams22.outboxer.api.observer.OrphansReclaimedInfo;
import io.github.bams22.outboxer.api.observer.PollCompletedInfo;
import io.github.bams22.outboxer.api.observer.PollerSaturatedInfo;
import io.github.bams22.outboxer.api.observer.RetentionPurgedInfo;
import io.github.bams22.outboxer.api.observer.StaleClaimsSweptInfo;
import io.github.bams22.outboxer.api.observer.StorageErrorInfo;
import io.github.bams22.outboxer.api.observer.StuckHandlerReclaimedInfo;
import io.github.bams22.outboxer.api.observer.WorkerRegisteredInfo;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.domain.WorkerInfo;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MicrometerOutboxListenerTest {

    private MeterRegistry registry;
    private MicrometerOutboxListener listener;

    @BeforeEach
    void setup() {
        registry = new SimpleMeterRegistry();
        listener = new MicrometerOutboxListener(registry);
    }

    @Test
    void publishedIncrementsPerEventTypeCounter() {
        listener.onEventPublished(published("ORDER"));
        listener.onEventPublished(published("ORDER"));
        listener.onEventPublished(published("EMAIL"));

        assertThat(
                        registry.counter("event_outboxer.events.published", "event_type", "ORDER")
                                .count())
                .isEqualTo(2.0);
        assertThat(
                        registry.counter("event_outboxer.events.published", "event_type", "EMAIL")
                                .count())
                .isEqualTo(1.0);
    }

    @Test
    void coalescedIncrementsPerTypeCounter() {
        listener.onEventCoalesced(new EventCoalescedInfo(UUID.randomUUID(), "ORDER", "order-1"));

        assertThat(
                        registry.counter("event_outboxer.events.coalesced", "event_type", "ORDER")
                                .count())
                .isEqualTo(1.0);
    }

    @Test
    void claimedRecordsQueueTime() {
        Instant createdAt = Instant.now();
        listener.onEventClaimed(
                new EventClaimedInfo(
                        UUID.randomUUID(),
                        "ORDER",
                        1,
                        createdAt,
                        createdAt.plusMillis(250),
                        new WorkerId("w-1")));

        assertThat(
                        registry.timer("event_outboxer.events.queue_time", "event_type", "ORDER")
                                .totalTime(TimeUnit.MILLISECONDS))
                .isEqualTo(250.0);
    }

    @Test
    void negativeQueueTimeIsClampedToZero() {
        Instant createdAt = Instant.now();
        listener.onEventClaimed(
                new EventClaimedInfo(
                        UUID.randomUUID(),
                        "ORDER",
                        1,
                        createdAt,
                        createdAt.minusSeconds(5),
                        new WorkerId("w-1")));

        assertThat(
                        registry.timer("event_outboxer.events.queue_time", "event_type", "ORDER")
                                .totalTime(TimeUnit.MILLISECONDS))
                .isEqualTo(0.0);
    }

    @Test
    void processedRecordsTimerAndAttempts() {
        listener.onEventProcessed(
                new EventProcessedInfo(UUID.randomUUID(), "ORDER", 1, Duration.ofMillis(150)));

        assertThat(
                        registry.timer(
                                        "event_outboxer.events.processing_time",
                                        "event_type",
                                        "ORDER")
                                .totalTime(TimeUnit.MILLISECONDS))
                .isEqualTo(150.0);
        assertThat(
                        registry.summary(
                                        "event_outboxer.events.attempts",
                                        "event_type",
                                        "ORDER",
                                        "outcome",
                                        "processed")
                                .mean())
                .isEqualTo(1.0);
    }

    @Test
    void retryScheduledIncrementsCounterTaggedByTrigger() {
        listener.onEventRetryScheduled(
                new EventRetryScheduledInfo(
                        UUID.randomUUID(),
                        "ORDER",
                        2,
                        Instant.now().plusSeconds(60),
                        EventRetryScheduledInfo.Trigger.FAILURE_DECISION,
                        "transient",
                        new RuntimeException("boom")));

        assertThat(
                        registry.counter(
                                        "event_outboxer.events.retry_scheduled",
                                        "event_type",
                                        "ORDER",
                                        "reason",
                                        "failure_decision")
                                .count())
                .isEqualTo(1.0);
    }

    @Test
    void disabledIncrementsCounterTaggedByTriggerAndRecordsAttempts() {
        listener.onEventDisabled(
                new EventDisabledInfo(
                        UUID.randomUUID(),
                        "ORDER",
                        10,
                        EventDisabledInfo.Trigger.FAILURE_DECISION,
                        "max-attempts",
                        null));

        assertThat(
                        registry.counter(
                                        "event_outboxer.events.disabled",
                                        "event_type",
                                        "ORDER",
                                        "reason",
                                        "failure_decision")
                                .count())
                .isEqualTo(1.0);
        assertThat(
                        registry.summary(
                                        "event_outboxer.events.attempts",
                                        "event_type",
                                        "ORDER",
                                        "outcome",
                                        "disabled")
                                .mean())
                .isEqualTo(10.0);
    }

    @Test
    void deletedIncrementsCounterAndRecordsAttempts() {
        listener.onEventDeleted(new EventDeletedInfo(UUID.randomUUID(), "ORDER", 3, "purged"));

        assertThat(registry.counter("event_outboxer.events.deleted", "event_type", "ORDER").count())
                .isEqualTo(1.0);
        assertThat(
                        registry.summary(
                                        "event_outboxer.events.attempts",
                                        "event_type",
                                        "ORDER",
                                        "outcome",
                                        "deleted")
                                .mean())
                .isEqualTo(3.0);
    }

    @Test
    void handlerErrorTagsByTypeAndExceptionClass() {
        listener.onHandlerError(
                new HandlerErrorInfo(
                        UUID.randomUUID(),
                        "ORDER",
                        1,
                        new IllegalStateException("x"),
                        Duration.ofMillis(250)));

        assertThat(
                        registry.counter(
                                        "event_outboxer.handler.errors",
                                        "event_type",
                                        "ORDER",
                                        "exception",
                                        "IllegalStateException")
                                .count())
                .isEqualTo(1.0);
        assertThat(
                        registry.timer("event_outboxer.handler.error_time", "event_type", "ORDER")
                                .totalTime(TimeUnit.MILLISECONDS))
                .isEqualTo(250.0);
    }

    @Test
    void workerRegisteredIncrementsGlobalCounter() {
        WorkerInfo info =
                WorkerInfo.builder()
                        .id(new WorkerId("w-1"))
                        .host("h-1")
                        .pid(100)
                        .metadata(Map.of())
                        .build();
        listener.onWorkerRegistered(new WorkerRegisteredInfo(info));

        assertThat(registry.counter("event_outboxer.workers.registered").count()).isEqualTo(1.0);
    }

    @Test
    void heartbeatFailedIncrementsCounter() {
        listener.onHeartbeatFailed(
                new HeartbeatFailedInfo(new WorkerId("w-1"), new RuntimeException("x")));

        assertThat(registry.counter("event_outboxer.heartbeat.failed").count()).isEqualTo(1.0);
    }

    @Test
    void orphansReclaimedIncrementsByEventCount() {
        listener.onOrphansReclaimed(
                new OrphansReclaimedInfo(
                        List.of(new WorkerId("dead-1"), new WorkerId("dead-2")), 7));

        assertThat(registry.counter("event_outboxer.orphans.reclaimed").count()).isEqualTo(7.0);
    }

    @Test
    void stuckHandlerReclaimedRecordsElapsed() {
        listener.onStuckHandlerReclaimed(
                new StuckHandlerReclaimedInfo(
                        UUID.randomUUID(),
                        "ORDER",
                        Duration.ofSeconds(90),
                        new WorkerId("w-1"),
                        true));

        assertThat(
                        registry.timer("event_outboxer.handler.stuck_time", "event_type", "ORDER")
                                .totalTime(TimeUnit.SECONDS))
                .isEqualTo(90.0);
    }

    @Test
    void handlerAbandonedIncrementsCounter() {
        listener.onHandlerAbandoned(
                new HandlerAbandonedInfo(
                        UUID.randomUUID(),
                        "ORDER",
                        new WorkerId("w-1"),
                        "outbox-ORDER-2",
                        Duration.ofMinutes(6),
                        true));

        assertThat(
                        registry.counter("event_outboxer.handler.abandoned", "event_type", "ORDER")
                                .count())
                .isEqualTo(1.0);
    }

    @Test
    void storageErrorTagsByOperation() {
        listener.onStorageError(new StorageErrorInfo("claim[ORDER]", new RuntimeException("x")));

        assertThat(
                        registry.counter(
                                        "event_outboxer.storage.errors",
                                        "operation",
                                        "claim[ORDER]")
                                .count())
                .isEqualTo(1.0);
    }

    @Test
    void pollCompletedTagsResultAndRecordsClaimTime() {
        listener.onPollCompleted(new PollCompletedInfo("ORDER", 10, 7, Duration.ofMillis(3)));
        listener.onPollCompleted(new PollCompletedInfo("ORDER", 10, 0, Duration.ofMillis(2)));

        assertThat(
                        registry.counter(
                                        "event_outboxer.poller.polls",
                                        "event_type",
                                        "ORDER",
                                        "result",
                                        "claimed")
                                .count())
                .isEqualTo(1.0);
        assertThat(
                        registry.counter(
                                        "event_outboxer.poller.polls",
                                        "event_type",
                                        "ORDER",
                                        "result",
                                        "empty")
                                .count())
                .isEqualTo(1.0);
        // Claim latency recorded for every poll, empty ones included.
        assertThat(
                        registry.timer("event_outboxer.poller.claim_time", "event_type", "ORDER")
                                .count())
                .isEqualTo(2L);
        assertThat(
                        registry.timer("event_outboxer.poller.claim_time", "event_type", "ORDER")
                                .totalTime(TimeUnit.MILLISECONDS))
                .isEqualTo(5.0);
    }

    @Test
    void pollerSaturatedIncrementsCounter() {
        listener.onPollerSaturated(new PollerSaturatedInfo("ORDER"));

        assertThat(
                        registry.counter("event_outboxer.poller.saturated", "event_type", "ORDER")
                                .count())
                .isEqualTo(1.0);
    }

    @Test
    void lockHoldTimerRecordsHeldDuration() {
        listener.onLockReleased(
                new LockReleasedInfo(UUID.randomUUID(), "ORDER", "k-1", Duration.ofMillis(12)));
        listener.onLockReleased(
                new LockReleasedInfo(UUID.randomUUID(), "ORDER", "k-2", Duration.ofMillis(8)));

        var held = registry.timer("event_outboxer.lock.hold_time", "event_type", "ORDER");
        assertThat(held.count()).isEqualTo(2L);
        assertThat(held.totalTime(TimeUnit.MILLISECONDS)).isEqualTo(20.0);
    }

    @Test
    void lockWaitTimerRecordsPerOutcome() {
        listener.onLockAcquired(
                new LockAcquiredInfo(UUID.randomUUID(), "ORDER", "k-1", Duration.ofMillis(7)));
        listener.onLockAcquired(
                new LockAcquiredInfo(UUID.randomUUID(), "ORDER", "k-2", Duration.ZERO));
        listener.onLockAcquisitionFailed(
                new LockAcquisitionInfo(
                        UUID.randomUUID(),
                        "ORDER",
                        "k-1",
                        LockAcquisitionInfo.Outcome.BUSY,
                        Duration.ofMillis(100),
                        null));
        listener.onLockAcquisitionFailed(
                new LockAcquisitionInfo(
                        UUID.randomUUID(),
                        "ORDER",
                        "k-1",
                        LockAcquisitionInfo.Outcome.ERROR,
                        Duration.ofMillis(3),
                        new RuntimeException("redis down")));

        var acquired =
                registry.timer(
                        "event_outboxer.lock.wait_time",
                        "event_type",
                        "ORDER",
                        "outcome",
                        "acquired");
        // Every acquisition is recorded, immediate ones included: _count is the acquisition total.
        assertThat(acquired.count()).isEqualTo(2L);
        assertThat(acquired.totalTime(TimeUnit.MILLISECONDS)).isEqualTo(7.0);
        var busy =
                registry.timer(
                        "event_outboxer.lock.wait_time", "event_type", "ORDER", "outcome", "busy");
        assertThat(busy.count()).isEqualTo(1L);
        assertThat(busy.totalTime(TimeUnit.MILLISECONDS)).isEqualTo(100.0);
        // A backend error is not a wait: the counter tags it, the timer does not.
        assertThat(registry.find("event_outboxer.lock.wait_time").tag("outcome", "error").timer())
                .isNull();
    }

    @Test
    void lockAcquisitionFailedTagsOutcome() {
        listener.onLockAcquisitionFailed(
                new LockAcquisitionInfo(
                        UUID.randomUUID(),
                        "ORDER",
                        "k-1",
                        LockAcquisitionInfo.Outcome.BUSY,
                        Duration.ofMillis(100),
                        null));
        listener.onLockAcquisitionFailed(
                new LockAcquisitionInfo(
                        UUID.randomUUID(),
                        "ORDER",
                        "k-1",
                        LockAcquisitionInfo.Outcome.ERROR,
                        Duration.ZERO,
                        new RuntimeException("redis down")));

        assertThat(
                        registry.counter(
                                        "event_outboxer.lock.acquisition_failed",
                                        "event_type",
                                        "ORDER",
                                        "outcome",
                                        "busy")
                                .count())
                .isEqualTo(1.0);
        assertThat(
                        registry.counter(
                                        "event_outboxer.lock.acquisition_failed",
                                        "event_type",
                                        "ORDER",
                                        "outcome",
                                        "error")
                                .count())
                .isEqualTo(1.0);
    }

    @Test
    void staleClaimsSweptIncrementsByCount() {
        listener.onStaleClaimsSwept(new StaleClaimsSweptInfo(12, Duration.ofMinutes(10)));

        assertThat(registry.counter("event_outboxer.claims.stale_swept").count()).isEqualTo(12.0);
    }

    @Test
    void retentionPurgedIncrementsPerKind() {
        listener.onRetentionPurged(new RetentionPurgedInfo(23, 4));
        listener.onRetentionPurged(new RetentionPurgedInfo(0, 6));

        assertThat(registry.counter("event_outboxer.retention.purged", "kind", "archive").count())
                .isEqualTo(23.0);
        assertThat(registry.counter("event_outboxer.retention.purged", "kind", "disabled").count())
                .isEqualTo(10.0);
    }

    @Test
    void maintenanceRunsTagTaskAndResult() {
        listener.onMaintenanceRunCompleted(
                new MaintenanceRunInfo("heartbeat", MaintenanceRunInfo.Result.OK, null));
        listener.onMaintenanceRunCompleted(
                new MaintenanceRunInfo(
                        "retention",
                        MaintenanceRunInfo.Result.FAILED,
                        new RuntimeException("boom")));

        assertThat(
                        registry.counter(
                                        "event_outboxer.maintenance.runs",
                                        "task",
                                        "heartbeat",
                                        "result",
                                        "ok")
                                .count())
                .isEqualTo(1.0);
        assertThat(
                        registry.counter(
                                        "event_outboxer.maintenance.runs",
                                        "task",
                                        "retention",
                                        "result",
                                        "failed")
                                .count())
                .isEqualTo(1.0);
    }

    @Test
    void customPrefixRespected() {
        MicrometerOutboxListener prefixed = new MicrometerOutboxListener(registry, "my.prefix");

        prefixed.onEventPublished(published("ORDER"));

        assertThat(registry.counter("my.prefix.events.published", "event_type", "ORDER").count())
                .isEqualTo(1.0);
    }

    private static EventPublishedInfo published(String type) {
        Instant now = Instant.now();
        return new EventPublishedInfo(UUID.randomUUID(), type, now, now, (short) 0);
    }
}
