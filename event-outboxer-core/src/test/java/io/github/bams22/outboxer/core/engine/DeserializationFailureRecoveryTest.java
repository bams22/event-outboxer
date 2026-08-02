/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.api.handle.builtin.FailureHandlers;
import io.github.bams22.outboxer.api.handle.builtin.MaxRetriesFailureHandler;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.observer.SerializationErrorInfo;
import io.github.bams22.outboxer.core.config.EventTypeConfig;
import io.github.bams22.outboxer.core.config.MaintenanceConfig;
import io.github.bams22.outboxer.core.publish.NoTransactionPolicy;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.exception.PayloadDeserializationException;
import io.github.bams22.outboxer.spi.EventSerializer;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryWorkerRegistry;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A payload deserialization failure must be recoverable: it routes through the FailureHandler chain
 * (retry with backoff, attempts consumed) instead of insta-disabling the event. The transient case
 * models mixed-version replicas during a rolling deploy; the permanent case (poison payload) still
 * ends in DISABLED once the attempt budget is exhausted.
 */
class DeserializationFailureRecoveryTest {

  private InMemoryEventStore store;
  private InMemoryWorkerRegistry registry;
  private OutboxEngine engine;

  @BeforeEach
  void setup() {
    store = new InMemoryEventStore();
    registry = new InMemoryWorkerRegistry();
  }

  @AfterEach
  void teardown() {
    if (engine != null && engine.isLifecycleActive()) {
      engine.stop(Duration.ofSeconds(2));
    }
  }

  @Test
  @DisplayName(
      "transient deserialization failure → retried through the chain and eventually handled")
  void transientFailureRecovers() {
    AtomicInteger deserializeCalls = new AtomicInteger();
    AtomicInteger handled = new AtomicInteger();
    AtomicInteger serializationErrors = new AtomicInteger();
    // First two deserialize calls fail (an outdated replica reading a new payload format),
    // then succeed (the updated replica claimed the retry).
    EventSerializer flaky =
        new EventSerializer() {
          @Override
          public String serialize(Object payload) {
            return String.valueOf(payload);
          }

          @Override
          @SuppressWarnings("unchecked")
          public <T> T deserialize(String payload, Class<T> type) {
            if (deserializeCalls.incrementAndGet() <= 2) {
              throw new PayloadDeserializationException("unknown property (simulated)", null);
            }
            return (T) payload;
          }
        };
    OutboxListener errorCounter =
        new OutboxListener() {
          @Override
          public void onEventSerializationError(SerializationErrorInfo info) {
            serializationErrors.incrementAndGet();
          }
        };
    engine =
        engineWith(flaky)
            .listener(errorCounter)
            // Small backoff so the retries land within the test budget.
            .defaultFailureHandler(
                FailureHandlers.builder()
                    .withMaxAttempts(10, MaxRetriesFailureHandler.ExhaustedAction.DISABLE)
                    .withFixedDelay(Duration.ofMillis(50)))
            .handler(
                handler(
                    "ROLL",
                    (ctx, payload) -> {
                      handled.incrementAndGet();
                      return EventOutcome.Success.INSTANCE;
                    }))
            .build();
    engine.start();

    UUID id = engine.publisher().publish("ROLL", "payload-v2");

    await().atMost(Duration.ofSeconds(10)).until(() -> store.findById(id).isEmpty());
    assertThat(handled).hasValue(1);
    assertThat(serializationErrors.get()).isEqualTo(2);
    assertThat(deserializeCalls.get()).isEqualTo(3);
  }

  @Test
  @DisplayName("poison payload → DISABLED only after the attempt budget, not on the first failure")
  void poisonPayloadDisabledAfterBudget() {
    AtomicInteger deserializeCalls = new AtomicInteger();
    EventSerializer alwaysBroken =
        new EventSerializer() {
          @Override
          public String serialize(Object payload) {
            return String.valueOf(payload);
          }

          @Override
          public <T> T deserialize(String payload, Class<T> type) {
            deserializeCalls.incrementAndGet();
            throw new PayloadDeserializationException("corrupt payload (simulated)", null);
          }
        };
    engine =
        engineWith(alwaysBroken)
            .defaultFailureHandler(
                FailureHandlers.builder()
                    .withMaxAttempts(3, MaxRetriesFailureHandler.ExhaustedAction.DISABLE)
                    .withFixedDelay(Duration.ofMillis(30)))
            .handler(handler("POISON", (ctx, payload) -> EventOutcome.Success.INSTANCE))
            .build();
    engine.start();

    UUID id = engine.publisher().publish("POISON", "garbage");

    await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> store.findById(id).map(e -> e.status() == EventStatus.DISABLED).orElse(false));
    Event disabled = store.findById(id).orElseThrow();
    assertThat(disabled.attempts()).isEqualTo(2);
    assertThat(deserializeCalls.get()).isEqualTo(3);
    assertThat(disabled.lastFailReason()).isNotBlank();
  }

  @Test
  @DisplayName(
      "transient store failure while finalizing the deserialization branch → row not stranded")
  void finalizeFailureInDeserializationBranchReleasesEvent() {
    AtomicInteger deserializeCalls = new AtomicInteger();
    // Deserialization fails once (routing enters the chain), then succeeds.
    EventSerializer flaky =
        new EventSerializer() {
          @Override
          public String serialize(Object payload) {
            return String.valueOf(payload);
          }

          @Override
          @SuppressWarnings("unchecked")
          public <T> T deserialize(String payload, Class<T> type) {
            if (deserializeCalls.incrementAndGet() == 1) {
              throw new PayloadDeserializationException("bad payload (simulated)", null);
            }
            return (T) payload;
          }
        };
    // The chain's markForRetry throws once: the protective catch must release the row instead
    // of leaving it PROCESSING.
    AtomicInteger retryFailures = new AtomicInteger(1);
    OutboxEngineRecoveryTest.DelegatingEventStore flakyStore =
        new OutboxEngineRecoveryTest.DelegatingEventStore(store) {
          @Override
          public boolean markForRetry(
              UUID id,
              io.github.bams22.outboxer.domain.WorkerId workerId,
              long claimedVersion,
              String reason,
              java.time.Instant runAt) {
            if (retryFailures.getAndDecrement() > 0) {
              throw new io.github.bams22.outboxer.domain.exception.EventStoreException(
                  "simulated transient markForRetry failure");
            }
            return super.markForRetry(id, workerId, claimedVersion, reason, runAt);
          }
        };
    AtomicInteger handled = new AtomicInteger();
    engine =
        engineWith(flaky)
            .eventStore(flakyStore)
            .handler(
                handler(
                    "FINFLAKY",
                    (ctx, payload) -> {
                      handled.incrementAndGet();
                      return EventOutcome.Success.INSTANCE;
                    }))
            .build();
    engine.start();

    UUID id = engine.publisher().publish("FINFLAKY", "payload");

    // Release path → re-claim → deserialization succeeds → handled. Nothing stays PROCESSING.
    await().atMost(Duration.ofSeconds(10)).until(() -> store.findById(id).isEmpty());
    assertThat(handled).hasValue(1);
    assertThat(store.metricsSnapshot().totalProcessing()).isZero();
  }

  // ---------------------------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------------------------

  private OutboxEngineBuilder engineWith(EventSerializer serializer) {
    EventTypeConfig fast =
        EventTypeConfig.defaults().toBuilder()
            .pollMinInterval(Duration.ofMillis(10))
            .pollMaxInterval(Duration.ofMillis(50))
            .pollMultiplier(1.1)
            .handlerMaxRuntime(Duration.ofSeconds(30))
            .build();
    MaintenanceConfig maintenance =
        MaintenanceConfig.builder()
            .heartbeatInterval(Duration.ofMillis(200))
            .deadThreshold(Duration.ofSeconds(5))
            .orphanRecoveryInterval(Duration.ofSeconds(60))
            .watchdogInterval(Duration.ofSeconds(1))
            .reclaimBatchSize(10)
            .shutdownTimeout(Duration.ofSeconds(2))
            .staleClaimSweepInterval(Duration.ofMinutes(5))
            .build();
    return new OutboxEngineBuilder()
        .eventStore(store)
        .workerRegistry(registry)
        .eventSerializer(serializer)
        .defaultEventTypeConfig(fast)
        .maintenance(maintenance)
        .noTransactionPolicy(NoTransactionPolicy.IGNORE)
        .includeLoggingListener(false);
  }

  @FunctionalInterface
  private interface HandleFn {
    EventOutcome apply(EventContext ctx, String payload);
  }

  private static EventHandler<String> handler(String type, HandleFn fn) {
    return new EventHandler<String>() {
      @Override
      public String eventType() {
        return type;
      }

      @Override
      public Class<String> payloadType() {
        return String.class;
      }

      @Override
      public EventOutcome handle(EventContext ctx, String payload) {
        return fn.apply(ctx, payload);
      }
    };
  }
}
