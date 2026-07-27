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

import io.github.bams22.outboxer.api.observer.DispatchRejectedInfo;
import io.github.bams22.outboxer.api.observer.EventClaimedInfo;
import io.github.bams22.outboxer.api.observer.EventDisabledInfo;
import io.github.bams22.outboxer.api.observer.EventProcessedInfo;
import io.github.bams22.outboxer.api.observer.EventPublishedInfo;
import io.github.bams22.outboxer.api.observer.EventRetryScheduledInfo;
import io.github.bams22.outboxer.api.observer.HandlerErrorInfo;
import io.github.bams22.outboxer.api.observer.HeartbeatFailedInfo;
import io.github.bams22.outboxer.api.observer.OrphansReclaimedInfo;
import io.github.bams22.outboxer.api.observer.StorageErrorInfo;
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

    assertThat(registry.counter("event_outboxer.events.published", "event_type", "ORDER").count())
        .isEqualTo(2.0);
    assertThat(registry.counter("event_outboxer.events.published", "event_type", "EMAIL").count())
        .isEqualTo(1.0);
  }

  @Test
  void claimedIncrementsPerTypeCounter() {
    listener.onEventClaimed(
        new EventClaimedInfo(UUID.randomUUID(), "ORDER", 1, Instant.now(), new WorkerId("w-1")));

    assertThat(registry.counter("event_outboxer.events.claimed", "event_type", "ORDER").count())
        .isEqualTo(1.0);
  }

  @Test
  void processedIncrementsCounterAndRecordsTimer() {
    listener.onEventProcessed(
        new EventProcessedInfo(UUID.randomUUID(), "ORDER", 1, Duration.ofMillis(150)));

    assertThat(registry.counter("event_outboxer.events.processed", "event_type", "ORDER").count())
        .isEqualTo(1.0);
    assertThat(
            registry
                .timer("event_outboxer.events.processing_time", "event_type", "ORDER")
                .totalTime(java.util.concurrent.TimeUnit.MILLISECONDS))
        .isEqualTo(150.0);
    assertThat(registry.summary("event_outboxer.events.attempts", "event_type", "ORDER").mean())
        .isEqualTo(1.0);
  }

  @Test
  void retryScheduledIncrementsCounter() {
    listener.onEventRetryScheduled(
        new EventRetryScheduledInfo(
            UUID.randomUUID(),
            "ORDER",
            2,
            Instant.now().plusSeconds(60),
            "transient",
            new RuntimeException("boom")));

    assertThat(
            registry
                .counter("event_outboxer.events.retry_scheduled", "event_type", "ORDER")
                .count())
        .isEqualTo(1.0);
  }

  @Test
  void disabledIncrementsCounter() {
    listener.onEventDisabled(
        new EventDisabledInfo(UUID.randomUUID(), "ORDER", 10, "max-attempts", null));

    assertThat(registry.counter("event_outboxer.events.disabled", "event_type", "ORDER").count())
        .isEqualTo(1.0);
  }

  @Test
  void handlerErrorTagsByType() {
    listener.onHandlerError(
        new HandlerErrorInfo(UUID.randomUUID(), "ORDER", 1, new RuntimeException("x")));

    assertThat(registry.counter("event_outboxer.handler.errors", "event_type", "ORDER").count())
        .isEqualTo(1.0);
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
  void orphansReclaimedTracksBothCounters() {
    listener.onOrphansReclaimed(
        new OrphansReclaimedInfo(List.of(new WorkerId("dead-1"), new WorkerId("dead-2")), 7));

    assertThat(registry.counter("event_outboxer.orphans.reclaimed").count()).isEqualTo(7.0);
    assertThat(registry.counter("event_outboxer.orphans.dead_workers").count()).isEqualTo(2.0);
  }

  @Test
  void storageErrorTagsByOperation() {
    listener.onStorageError(new StorageErrorInfo("claim[ORDER]", new RuntimeException("x")));

    assertThat(
            registry.counter("event_outboxer.storage.errors", "operation", "claim[ORDER]").count())
        .isEqualTo(1.0);
  }

  @Test
  void dispatchRejectedTagsByType() {
    listener.onDispatchRejected(
        new DispatchRejectedInfo(UUID.randomUUID(), "ORDER", new RuntimeException("saturated")));

    assertThat(registry.counter("event_outboxer.dispatch.rejected", "event_type", "ORDER").count())
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
