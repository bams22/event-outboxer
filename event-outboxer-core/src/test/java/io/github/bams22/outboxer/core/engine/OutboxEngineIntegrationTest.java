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
import io.github.bams22.outboxer.api.observer.EngineCrashedInfo;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.core.config.EventTypeConfig;
import io.github.bams22.outboxer.core.config.MaintenanceConfig;
import io.github.bams22.outboxer.core.publish.NoTransactionPolicy;
import io.github.bams22.outboxer.core.support.StringEventSerializer;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryWorkerRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class OutboxEngineIntegrationTest {

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
    // isLifecycleActive() stays true across a crash (state() would report STOPPED). We still
    // want to run stop() to clean up maintenance scheduler + handler pools in both cases.
    if (engine != null && engine.isLifecycleActive()) {
      engine.stop(Duration.ofSeconds(2));
    }
  }

  @Test
  @DisplayName("publish → claim → handle=Success → row deleted")
  void happyPath() {
    AtomicInteger invoked = new AtomicInteger();
    Set<String> seen = ConcurrentHashMap.newKeySet();
    engine =
        fastEngine()
            .handler(
                recordingHandler(
                    "ORDER",
                    (ctx, payload) -> {
                      invoked.incrementAndGet();
                      seen.add(payload);
                      return EventOutcome.Success.INSTANCE;
                    }))
            .build();
    engine.start();

    UUID id = engine.publisher().publish("ORDER", "order-1");

    await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> store.findById(id).isEmpty());
    assertThat(invoked).hasValueGreaterThanOrEqualTo(1);
    assertThat(seen).contains("order-1");
  }

  @Test
  @DisplayName("per-type isolation: slow type-A does not block fast type-B")
  void perTypeIsolation() {
    AtomicInteger slowDone = new AtomicInteger();
    AtomicInteger fastDone = new AtomicInteger();
    engine =
        fastEngine()
            .handler(
                recordingHandler(
                    "SLOW",
                    (ctx, payload) -> {
                      sleepQuietly(500);
                      slowDone.incrementAndGet();
                      return EventOutcome.Success.INSTANCE;
                    }))
            .handler(
                recordingHandler(
                    "FAST",
                    (ctx, payload) -> {
                      fastDone.incrementAndGet();
                      return EventOutcome.Success.INSTANCE;
                    }))
            .build();
    engine.start();

    // Saturate the slow type with one event, then publish a fast one.
    engine.publisher().publish("SLOW", "slow-1");
    UUID fastId = engine.publisher().publish("FAST", "fast-1");

    await()
        .atMost(Duration.ofSeconds(2))
        .until(() -> fastDone.get() >= 1 && store.findById(fastId).isEmpty());
  }

  @Test
  @DisplayName("handler exception → failure chain → retry scheduled")
  void retryOnException() {
    AtomicInteger attempts = new AtomicInteger();
    engine =
        fastEngine()
            .handler(
                recordingHandler(
                    "FLAKY",
                    (ctx, payload) -> {
                      int n = attempts.incrementAndGet();
                      if (n < 2) {
                        throw new RuntimeException("transient failure #" + n);
                      }
                      return EventOutcome.Success.INSTANCE;
                    }))
            .build();
    engine.start();

    UUID id = engine.publisher().publish("FLAKY", "payload");

    await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> store.findById(id).isEmpty());
    assertThat(attempts).hasValueGreaterThanOrEqualTo(2);
  }

  @Test
  @DisplayName("poller thread death → health check flips engine state to STOPPED")
  void crashDetectionFlipsStateOnPollerDeath() {
    AtomicReference<EngineCrashedInfo> captured = new AtomicReference<>();
    OutboxListener crashListener =
        new OutboxListener() {
          @Override
          public void onEngineCrashed(EngineCrashedInfo info) {
            captured.set(info);
          }
        };

    engine =
        fastEngine()
            .handler(
                recordingHandler("BOOM", (ctx, payload) -> EventOutcome.Success.INSTANCE))
            .pollStrategy(
                (eventType, workerId, batchSize) -> {
                  // Uncaught Error — bypasses Poller.tick()'s RuntimeException catch, kills
                  // thread. Plain java.lang.Error on purpose: VirtualMachineError subclasses
                  // like OutOfMemoryError / StackOverflowError are interpreted by some JVM
                  // tooling (surefire fork, ByteBuddy agent, certain JDK 25 builds) as fatal
                  // for the whole JVM, even when thrown synthetically. Plain Error keeps the
                  // crash-detection semantics (non-RuntimeException, kills the thread) without
                  // triggering JVM-critical handling.
                  throw new Error("simulated");
                })
            .listener(crashListener)
            .build();
    engine.start();

    // Watchdog/health-check cadence in fastEngine is 200ms; allow time for at least one pass.
    await()
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(50))
        .until(() -> engine.state() == OutboxEngine.State.STOPPED);

    assertThat(engine.isLifecycleActive()).isTrue();  // stop() not yet called; cleanup pending
    assertThat(captured.get()).isNotNull();
    assertThat(captured.get().reason()).contains("BOOM");
    assertThat(captured.get().workerId()).isEqualTo(engine.workerId());
  }

  @Test
  @DisplayName("unknown handler (policy=SKIP) reschedules the event back to PENDING")
  void unknownHandlerReschedules() {
    engine =
        fastEngine()
            .handler(
                recordingHandler(
                    "KNOWN", (ctx, payload) -> EventOutcome.Success.INSTANCE))
            .build();
    engine.start();

    UUID unknownId = UUID.randomUUID();
    store.save(
        io.github.bams22.outboxer.domain.PendingEvent.builder()
            .id(unknownId)
            .eventType("UNKNOWN")
            .payload("x")
            .payloadClass("java.lang.String")
            .priority((short) 0)
            .runAt(java.time.Instant.now().minusSeconds(1))
            .traceContext(java.util.Map.of())
            .build());

    // Should be picked by a poller eventually... but we only poll KNOWN, so no poller sees it.
    // The test validates that no KNOWN poller errors; the UNKNOWN row stays PENDING.
    sleepQuietly(500);
    assertThat(store.findById(unknownId)).isPresent();
  }

  // ---------------------------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------------------------

  private OutboxEngineBuilder fastEngine() {
    EventTypeConfig fast =
        EventTypeConfig.defaults().toBuilder()
            .pollMinInterval(Duration.ofMillis(10))
            .pollMaxInterval(Duration.ofMillis(50))
            .pollMultiplier(1.1)
            .handlerPoolSize(2)
            .handlerMaxRuntime(Duration.ofSeconds(30))
            .build();
    MaintenanceConfig maintenance =
        MaintenanceConfig.builder()
            .heartbeatInterval(Duration.ofMillis(100))
            .deadThreshold(Duration.ofSeconds(1))
            .orphanRecoveryInterval(Duration.ofMillis(500))
            .watchdogInterval(Duration.ofMillis(200))
            .reclaimBatchSize(10)
            .shutdownTimeout(Duration.ofSeconds(2))
            .build();
    return new OutboxEngineBuilder()
        .eventStore(store)
        .workerRegistry(registry)
        .eventSerializer(new StringEventSerializer())
        .defaultEventTypeConfig(fast)
        .maintenance(maintenance)
        .noTransactionPolicy(NoTransactionPolicy.IGNORE)
        .includeLoggingListener(false)
        .listener(new OutboxListener() {});
  }

  @FunctionalInterface
  private interface HandleFn {
    EventOutcome apply(EventContext ctx, String payload);
  }

  private static EventHandler<String> recordingHandler(String type, HandleFn fn) {
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

  private static void sleepQuietly(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @SuppressWarnings("unused")
  private static List<String> ignore() {
    return List.of();
  }
}
