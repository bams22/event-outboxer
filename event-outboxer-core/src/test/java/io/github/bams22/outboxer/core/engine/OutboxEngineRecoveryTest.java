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
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.core.config.EventTypeConfig;
import io.github.bams22.outboxer.core.config.MaintenanceConfig;
import io.github.bams22.outboxer.core.publish.NoTransactionPolicy;
import io.github.bams22.outboxer.core.support.StringEventSerializer;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryWorkerRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Recovery-path coverage: every way a claimed event could historically get stranded in PROCESSING
 * while the worker stays alive, plus engine restartability. Companion to the happy paths in {@link
 * OutboxEngineIntegrationTest}.
 */
class OutboxEngineRecoveryTest {

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
      "rejected dispatch (saturated executor) → event released back to PENDING and eventually"
          + " processed")
  void rejectedDispatchReleasesEvent() throws Exception {
    CountDownLatch gate = new CountDownLatch(1);
    AtomicInteger done = new AtomicInteger();
    // pool=1, queue=0 (synchronous handoff): one in-flight handler, everything else rejected.
    engine =
        fastEngine(cfg -> cfg.handlerPoolSize(1).handlerQueueCapacity(0).claimBatchSize(5))
            .handler(
                handler(
                    "BURST",
                    (ctx, payload) -> {
                      awaitQuietly(gate);
                      done.incrementAndGet();
                      return EventOutcome.Success.INSTANCE;
                    }))
            .build();
    engine.start();

    for (int i = 0; i < 5; i++) {
      engine.publisher().publish("BURST", "burst-" + i);
    }

    // Give the poller time to claim the batch and reject 4 of the 5 dispatches, then let the
    // single in-flight handler finish. Before the release fix the rejected 4 stayed PROCESSING
    // forever and this await timed out.
    Thread.sleep(300);
    gate.countDown();

    await().atMost(Duration.ofSeconds(10)).until(() -> done.get() >= 5);
    await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> store.metricsSnapshot().totalProcessing() == 0);
  }

  @Test
  @DisplayName(
      "graceful shutdown with unfinished handlers → leftover claims released, nothing stays"
          + " PROCESSING")
  void shutdownReleasesUnfinishedClaims() {
    AtomicBoolean started = new AtomicBoolean();
    engine =
        fastEngine(cfg -> cfg.handlerPoolSize(1).handlerQueueCapacity(10).claimBatchSize(5))
            .handler(
                handler(
                    "STUCK",
                    (ctx, payload) -> {
                      started.set(true);
                      sleepIgnoringInterrupts(10_000);
                      return EventOutcome.Success.INSTANCE;
                    }))
            .build();
    engine.start();

    for (int i = 0; i < 3; i++) {
      engine.publisher().publish("STUCK", "stuck-" + i);
    }
    await().atMost(Duration.ofSeconds(5)).untilTrue(started);

    // Drain times out (handler ignores the interrupt), stop() must release every claimed row.
    engine.stop(Duration.ofMillis(300));

    assertThat(store.metricsSnapshot().totalProcessing()).isZero();
    assertThat(store.metricsSnapshot().totalPending()).isEqualTo(3);
  }

  @Test
  @DisplayName("engine restart: stop() then start() processes new events with fresh executors")
  void engineIsRestartable() {
    AtomicInteger done = new AtomicInteger();
    engine =
        fastEngine(cfg -> cfg)
            .handler(
                handler(
                    "CYCLE",
                    (ctx, payload) -> {
                      done.incrementAndGet();
                      return EventOutcome.Success.INSTANCE;
                    }))
            .build();

    engine.start();
    UUID first = engine.publisher().publish("CYCLE", "one");
    await().atMost(Duration.ofSeconds(5)).until(() -> store.findById(first).isEmpty());

    engine.stop(Duration.ofSeconds(2));
    engine.start();

    UUID second = engine.publisher().publish("CYCLE", "two");
    await().atMost(Duration.ofSeconds(5)).until(() -> store.findById(second).isEmpty());
    assertThat(done).hasValueGreaterThanOrEqualTo(2);
  }

  @Test
  @DisplayName("transient finalize failure → event released and redelivered instead of stranded")
  void finalizeFailureReleasesEvent() {
    AtomicInteger handled = new AtomicInteger();
    FlakyFinalizeStore flaky = new FlakyFinalizeStore(store, 1);
    engine =
        fastEngineOn(flaky, cfg -> cfg)
            .handler(
                handler(
                    "FINFAIL",
                    (ctx, payload) -> {
                      handled.incrementAndGet();
                      return EventOutcome.Success.INSTANCE;
                    }))
            .build();
    engine.start();

    UUID id = engine.publisher().publish("FINFAIL", "payload");

    // First markProcessed throws → dispatcher releases the row → poller re-claims → second
    // markProcessed succeeds. At-least-once: the handler runs (at least) twice.
    await().atMost(Duration.ofSeconds(10)).until(() -> store.findById(id).isEmpty());
    assertThat(handled).hasValueGreaterThanOrEqualTo(2);
  }

  @Test
  @DisplayName("busy entity lock → event re-scheduled without consuming the attempts budget")
  void lockBusyDoesNotBurnAttempts() {
    AtomicInteger locks = new AtomicInteger();
    EntityLocker busyThenFree =
        (key, ttl) -> locks.incrementAndGet() <= 3 ? Optional.empty() : Optional.of(() -> {});
    AtomicInteger observedAttempts = new AtomicInteger(-1);
    engine =
        fastEngine(cfg -> cfg)
            .entityLocker(busyThenFree)
            .handler(
                new EventHandler<String>() {
                  @Override
                  public String eventType() {
                    return "LOCKED";
                  }

                  @Override
                  public Class<String> payloadType() {
                    return String.class;
                  }

                  @Override
                  public String extractLockKey(String payload) {
                    return "hot-key";
                  }

                  @Override
                  public EventOutcome handle(EventContext ctx, String payload) {
                    observedAttempts.set(ctx.attempt());
                    return EventOutcome.Success.INSTANCE;
                  }
                })
            .build();
    engine.start();

    UUID id = engine.publisher().publish("LOCKED", "payload");

    await().atMost(Duration.ofSeconds(10)).until(() -> store.findById(id).isEmpty());
    // Three lock-busy passes must not increment attempts: the one real execution is attempt 1.
    assertThat(observedAttempts).hasValue(1);
    assertThat(locks.get()).isGreaterThanOrEqualTo(4);
  }

  @Test
  @DisplayName("heartbeat re-registers the worker after a peer reaped its registry row")
  void heartbeatReRegistersReapedWorker() {
    engine =
        fastEngine(cfg -> cfg)
            .handler(handler("NOOP", (ctx, payload) -> EventOutcome.Success.INSTANCE))
            .build();
    engine.start();
    WorkerId id = engine.workerId();
    assertThat(registry.findById(id)).isPresent();

    // Simulate a peer's orphan recovery reaping this live worker after a long pause.
    registry.deregister(id);
    assertThat(registry.findById(id)).isEmpty();

    // The next heartbeat tick (100ms cadence in fastEngine) must restore the row.
    await().atMost(Duration.ofSeconds(5)).until(() -> registry.findById(id).isPresent());
  }

  // ---------------------------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------------------------

  private interface TypeCfg {
    EventTypeConfig.EventTypeConfigBuilder apply(EventTypeConfig.EventTypeConfigBuilder b);
  }

  private OutboxEngineBuilder fastEngine(TypeCfg customize) {
    return fastEngineOn(store, customize);
  }

  private OutboxEngineBuilder fastEngineOn(
      io.github.bams22.outboxer.spi.EventStore eventStore, TypeCfg customize) {
    EventTypeConfig fast =
        customize
            .apply(
                EventTypeConfig.defaults().toBuilder()
                    .pollMinInterval(Duration.ofMillis(10))
                    .pollMaxInterval(Duration.ofMillis(50))
                    .pollMultiplier(1.1)
                    .handlerMaxRuntime(Duration.ofSeconds(30)))
            .build();
    MaintenanceConfig maintenance =
        MaintenanceConfig.builder()
            .heartbeatInterval(Duration.ofMillis(100))
            .deadThreshold(Duration.ofSeconds(5))
            .orphanRecoveryInterval(Duration.ofSeconds(60))
            .watchdogInterval(Duration.ofMillis(200))
            .reclaimBatchSize(10)
            .shutdownTimeout(Duration.ofSeconds(2))
            .staleClaimSweepInterval(Duration.ofMinutes(5))
            .build();
    return new OutboxEngineBuilder()
        .eventStore(eventStore)
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

  private static void awaitQuietly(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static void sleepIgnoringInterrupts(long ms) {
    long deadline = System.currentTimeMillis() + ms;
    while (System.currentTimeMillis() < deadline) {
      try {
        Thread.sleep(deadline - System.currentTimeMillis());
      } catch (InterruptedException _) {
        // deliberately keep sleeping: simulates a handler that outlives the drain timeout
      }
    }
  }

  /**
   * Delegating store whose {@code markProcessed} throws for the first N calls — simulates a
   * transient storage error at finalize time.
   */
  private static final class FlakyFinalizeStore extends DelegatingEventStore {

    private final AtomicInteger failuresLeft;

    FlakyFinalizeStore(io.github.bams22.outboxer.spi.EventStore delegate, int failures) {
      super(delegate);
      this.failuresLeft = new AtomicInteger(failures);
    }

    @Override
    public boolean markProcessed(UUID id, WorkerId workerId, long claimedVersion) {
      if (failuresLeft.getAndDecrement() > 0) {
        throw new io.github.bams22.outboxer.domain.exception.EventStoreException(
            "simulated transient finalize failure");
      }
      return super.markProcessed(id, workerId, claimedVersion);
    }
  }

  /**
   * Forwards every {@code EventStore} call to a delegate; test doubles override single methods.
   * Package-private so sibling engine tests can reuse it.
   */
  static class DelegatingEventStore implements io.github.bams22.outboxer.spi.EventStore {

    private final io.github.bams22.outboxer.spi.EventStore delegate;

    DelegatingEventStore(io.github.bams22.outboxer.spi.EventStore delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean save(io.github.bams22.outboxer.domain.PendingEvent event) {
      return delegate.save(event);
    }

    @Override
    public Optional<UUID> lockPendingByDedupKey(String eventType, String dedupKey) {
      return delegate.lockPendingByDedupKey(eventType, dedupKey);
    }

    @Override
    public void saveAll(List<io.github.bams22.outboxer.domain.PendingEvent> events) {
      delegate.saveAll(events);
    }

    @Override
    public List<io.github.bams22.outboxer.domain.ClaimedEvent> claim(
        io.github.bams22.outboxer.spi.ClaimRequest request) {
      return delegate.claim(request);
    }

    @Override
    public boolean markProcessed(UUID id, WorkerId workerId, long claimedVersion) {
      return delegate.markProcessed(id, workerId, claimedVersion);
    }

    @Override
    public boolean markForRetry(
        UUID id, WorkerId workerId, long claimedVersion, String reason, Instant runAt) {
      return delegate.markForRetry(id, workerId, claimedVersion, reason, runAt);
    }

    @Override
    public boolean release(
        UUID id, WorkerId workerId, long claimedVersion, String reason, Instant runAt) {
      return delegate.release(id, workerId, claimedVersion, reason, runAt);
    }

    @Override
    public int releaseClaimed(WorkerId workerId, Instant now) {
      return delegate.releaseClaimed(workerId, now);
    }

    @Override
    public boolean markDisabled(UUID id, WorkerId workerId, long claimedVersion, String reason) {
      return delegate.markDisabled(id, workerId, claimedVersion, reason);
    }

    @Override
    public boolean forceReclaim(UUID id, WorkerId workerId, long claimedVersion, Instant runAt) {
      return delegate.forceReclaim(id, workerId, claimedVersion, runAt);
    }

    @Override
    public int sweepStale(java.time.Duration olderThan, int limit) {
      return delegate.sweepStale(olderThan, limit);
    }

    @Override
    public int reclaimOrphans(List<WorkerId> deadWorkers, Instant now) {
      return delegate.reclaimOrphans(deadWorkers, now);
    }

    @Override
    public Optional<io.github.bams22.outboxer.domain.Event> findById(UUID id) {
      return delegate.findById(id);
    }

    @Override
    public io.github.bams22.outboxer.spi.OutboxMetricsSnapshot metricsSnapshot() {
      return delegate.metricsSnapshot();
    }
  }
}
