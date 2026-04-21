/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spi.contracts;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.OutboxMetricsSnapshot;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Reusable contract specification for every {@link EventStore} implementation. Subclasses provide
 * a fresh store via {@link #newStore()}; each test executes against its own isolated store so that
 * tests do not share state.
 *
 * <p>The contract covers the full happy-path lifecycle (publish → claim → finalize), every
 * optimistic-lock failure mode (stale version, different worker) per ADR-0014, orphan recovery,
 * watchdog force-reclaim, and a high-concurrency invariant that SKIP-LOCKED-like semantics produce
 * no duplicate claims across workers.
 */
public abstract class AbstractEventStoreContractTest {

  protected static final String EVENT_TYPE_A = "TYPE_A";
  protected static final String EVENT_TYPE_B = "TYPE_B";
  protected static final WorkerId WORKER_1 = new WorkerId("worker-1");
  protected static final WorkerId WORKER_2 = new WorkerId("worker-2");

  protected EventStore store;

  /** Build a fresh {@link EventStore} backed by whatever state the adapter needs. */
  protected abstract EventStore newStore();

  @BeforeEach
  void setUpStore() {
    store = newStore();
  }

  // ---------------------------------------------------------------------------------------------
  // save / saveAll / findById
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("save() persists a pending event and findById() returns it unchanged")
  void save_roundTrip() {
    PendingEvent pending = pending(EVENT_TYPE_A, "hello", Instant.now());

    store.save(pending);

    Optional<Event> found = store.findById(pending.id());
    assertThat(found).isPresent();
    Event event = found.orElseThrow();
    assertThat(event.id()).isEqualTo(pending.id());
    assertThat(event.eventType()).isEqualTo(EVENT_TYPE_A);
    assertThat(event.payload()).isEqualTo(jsonString("hello"));
    assertThat(event.status()).isEqualTo(EventStatus.PENDING);
    assertThat(event.attempts()).isZero();
    assertThat(event.version()).isGreaterThanOrEqualTo(0L);
    assertThat(event.claimedBy()).isNull();
    assertThat(event.claimedAt()).isNull();
    assertThat(event.lastFailReason()).isNull();
  }

  @Test
  @DisplayName("saveAll() persists every event in the batch")
  void saveAll_persistsAll() {
    PendingEvent p1 = pending(EVENT_TYPE_A, "p1", Instant.now());
    PendingEvent p2 = pending(EVENT_TYPE_A, "p2", Instant.now());
    PendingEvent p3 = pending(EVENT_TYPE_B, "p3", Instant.now());

    store.saveAll(List.of(p1, p2, p3));

    assertThat(store.findById(p1.id())).isPresent();
    assertThat(store.findById(p2.id())).isPresent();
    assertThat(store.findById(p3.id())).isPresent();
  }

  @Test
  @DisplayName("findById() returns empty for an unknown id")
  void findById_empty_whenMissing() {
    assertThat(store.findById(UUID.randomUUID())).isEmpty();
  }

  // ---------------------------------------------------------------------------------------------
  // claim
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("claim() returns eligible events ordered by priority DESC then runAt ASC")
  void claim_ordersByPriorityThenRunAt() {
    Instant base = Instant.now().minusSeconds(60);

    PendingEvent loPrioOld = pending(EVENT_TYPE_A, "lo-old", base, (short) 0);
    PendingEvent hiPrioNew = pending(EVENT_TYPE_A, "hi-new", base.plusSeconds(10), (short) 10);
    PendingEvent hiPrioOld = pending(EVENT_TYPE_A, "hi-old", base.minusSeconds(5), (short) 10);
    store.save(loPrioOld);
    store.save(hiPrioNew);
    store.save(hiPrioOld);

    List<ClaimedEvent> claimed = store.claim(new ClaimRequest(EVENT_TYPE_A, WORKER_1, 10));

    assertThat(claimed)
        .extracting(ClaimedEvent::payload)
        .containsExactly(jsonString("hi-old"), jsonString("hi-new"), jsonString("lo-old"));
  }

  @Test
  @DisplayName("claim() respects the limit parameter")
  void claim_respectsLimit() {
    Instant past = Instant.now().minusSeconds(1);
    for (int i = 0; i < 10; i++) {
      store.save(pending(EVENT_TYPE_A, "p" + i, past));
    }

    List<ClaimedEvent> claimed = store.claim(new ClaimRequest(EVENT_TYPE_A, WORKER_1, 3));

    assertThat(claimed).hasSize(3);
  }

  @Test
  @DisplayName("claim() skips events whose runAt is still in the future")
  void claim_skipsFutureRunAt() {
    Instant future = Instant.now().plus(Duration.ofHours(1));
    store.save(pending(EVENT_TYPE_A, "later", future));

    List<ClaimedEvent> claimed = store.claim(new ClaimRequest(EVENT_TYPE_A, WORKER_1, 10));

    assertThat(claimed).isEmpty();
  }

  @Test
  @DisplayName("claim() ignores events belonging to other event types")
  void claim_scopedToEventType() {
    Instant past = Instant.now().minusSeconds(1);
    PendingEvent a = pending(EVENT_TYPE_A, "a", past);
    PendingEvent b = pending(EVENT_TYPE_B, "b", past);
    store.save(a);
    store.save(b);

    List<ClaimedEvent> claimedA = store.claim(new ClaimRequest(EVENT_TYPE_A, WORKER_1, 10));

    assertThat(claimedA).hasSize(1);
    assertThat(claimedA.get(0).id()).isEqualTo(a.id());
  }

  @Test
  @DisplayName("claim() transitions rows to PROCESSING and records claimedBy / claimedAt / version")
  void claim_transitionsToProcessing() {
    PendingEvent p = pending(EVENT_TYPE_A, "x", Instant.now().minusSeconds(1));
    store.save(p);
    long initialVersion = store.findById(p.id()).orElseThrow().version();

    List<ClaimedEvent> claimed = store.claim(new ClaimRequest(EVENT_TYPE_A, WORKER_1, 10));

    assertThat(claimed).hasSize(1);
    ClaimedEvent ce = claimed.get(0);
    assertThat(ce.claimedVersion()).isGreaterThan(initialVersion);

    Event after = store.findById(p.id()).orElseThrow();
    assertThat(after.status()).isEqualTo(EventStatus.PROCESSING);
    assertThat(after.claimedBy()).isEqualTo(WORKER_1);
    assertThat(after.claimedAt()).isNotNull();
    assertThat(after.version()).isEqualTo(ce.claimedVersion());
  }

  @Test
  @DisplayName("claim() on an empty store returns an empty list")
  void claim_emptyStore_returnsEmptyList() {
    assertThat(store.claim(new ClaimRequest(EVENT_TYPE_A, WORKER_1, 10))).isEmpty();
  }

  @Test
  @DisplayName("claim() from concurrent workers never returns the same row twice")
  void claim_concurrent_noDuplicates() throws Exception {
    int total = 500;
    Instant past = Instant.now().minusSeconds(1);
    for (int i = 0; i < total; i++) {
      store.save(pending(EVENT_TYPE_A, "p" + i, past));
    }

    int workers = 16;
    ExecutorService exec = Executors.newFixedThreadPool(workers);
    try {
      CountDownLatch go = new CountDownLatch(1);
      Set<UUID> allClaimed = ConcurrentHashMap.newKeySet();
      AtomicInteger duplicates = new AtomicInteger();
      List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
      for (int w = 0; w < workers; w++) {
        WorkerId wid = new WorkerId("w-" + w);
        futures.add(
            exec.submit(
                () -> {
                  try {
                    go.await();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                  }
                  int drained = 0;
                  while (drained < total) {
                    List<ClaimedEvent> batch = store.claim(new ClaimRequest(EVENT_TYPE_A, wid, 7));
                    if (batch.isEmpty()) {
                      break;
                    }
                    for (ClaimedEvent ce : batch) {
                      if (!allClaimed.add(ce.id())) {
                        duplicates.incrementAndGet();
                      }
                    }
                    drained += batch.size();
                  }
                }));
      }
      go.countDown();
      for (var f : futures) {
        f.get(30, TimeUnit.SECONDS);
      }
      assertThat(duplicates).hasValue(0);
      assertThat(allClaimed).hasSize(total);
    } finally {
      exec.shutdownNow();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // markProcessed
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("markProcessed() returns true and removes the row when version + worker match")
  void markProcessed_removesRow_onMatch() {
    ClaimedEvent claimed = publishAndClaim(EVENT_TYPE_A, "x", WORKER_1);

    boolean ok = store.markProcessed(claimed.id(), WORKER_1, claimed.claimedVersion());

    assertThat(ok).isTrue();
    assertThat(store.findById(claimed.id())).isEmpty();
  }

  @Test
  @DisplayName("markProcessed() returns false on a stale version")
  void markProcessed_false_onStaleVersion() {
    ClaimedEvent claimed = publishAndClaim(EVENT_TYPE_A, "x", WORKER_1);

    boolean ok = store.markProcessed(claimed.id(), WORKER_1, claimed.claimedVersion() - 1);

    assertThat(ok).isFalse();
    assertThat(store.findById(claimed.id())).isPresent();
  }

  @Test
  @DisplayName("markProcessed() returns false when the caller is a different worker")
  void markProcessed_false_onDifferentWorker() {
    ClaimedEvent claimed = publishAndClaim(EVENT_TYPE_A, "x", WORKER_1);

    boolean ok = store.markProcessed(claimed.id(), WORKER_2, claimed.claimedVersion());

    assertThat(ok).isFalse();
    assertThat(store.findById(claimed.id())).isPresent();
  }

  // ---------------------------------------------------------------------------------------------
  // markForRetry
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("markForRetry() reverts to PENDING, clears claim, bumps attempts and version")
  void markForRetry_revertsToPending() {
    ClaimedEvent claimed = publishAndClaim(EVENT_TYPE_A, "x", WORKER_1);
    // Truncate to microseconds: TIMESTAMPTZ in PG is microsecond-precision.
    Instant nextRun = Instant.now().plusSeconds(60).truncatedTo(java.time.temporal.ChronoUnit.MICROS);

    boolean ok =
        store.markForRetry(claimed.id(), WORKER_1, claimed.claimedVersion(), "transient", nextRun);

    assertThat(ok).isTrue();
    Event after = store.findById(claimed.id()).orElseThrow();
    assertThat(after.status()).isEqualTo(EventStatus.PENDING);
    assertThat(after.claimedBy()).isNull();
    assertThat(after.claimedAt()).isNull();
    assertThat(after.attempts()).isEqualTo(claimed.attempts() + 1);
    assertThat(after.version()).isGreaterThan(claimed.claimedVersion());
    assertThat(after.lastFailReason()).isEqualTo("transient");
    assertThat(after.runAt()).isEqualTo(nextRun);
  }

  @Test
  @DisplayName("markForRetry() returns false on a stale version")
  void markForRetry_false_onStaleVersion() {
    ClaimedEvent claimed = publishAndClaim(EVENT_TYPE_A, "x", WORKER_1);

    boolean ok =
        store.markForRetry(
            claimed.id(),
            WORKER_1,
            claimed.claimedVersion() - 1,
            "no-op",
            Instant.now().plusSeconds(10));

    assertThat(ok).isFalse();
  }

  // ---------------------------------------------------------------------------------------------
  // markDisabled
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("markDisabled() moves the event to DISABLED and records the reason")
  void markDisabled_status_becomesDisabled() {
    ClaimedEvent claimed = publishAndClaim(EVENT_TYPE_A, "x", WORKER_1);

    boolean ok =
        store.markDisabled(claimed.id(), WORKER_1, claimed.claimedVersion(), "exhausted");

    assertThat(ok).isTrue();
    Event after = store.findById(claimed.id()).orElseThrow();
    assertThat(after.status()).isEqualTo(EventStatus.DISABLED);
    assertThat(after.claimedBy()).isNull();
    assertThat(after.claimedAt()).isNull();
    assertThat(after.lastFailReason()).isEqualTo("exhausted");
    assertThat(after.version()).isGreaterThan(claimed.claimedVersion());
  }

  @Test
  @DisplayName("markDisabled() returns false on a stale version")
  void markDisabled_false_onStaleVersion() {
    ClaimedEvent claimed = publishAndClaim(EVENT_TYPE_A, "x", WORKER_1);

    boolean ok =
        store.markDisabled(claimed.id(), WORKER_1, claimed.claimedVersion() - 1, "stale");

    assertThat(ok).isFalse();
    assertThat(store.findById(claimed.id()).orElseThrow().status())
        .isEqualTo(EventStatus.PROCESSING);
  }

  // ---------------------------------------------------------------------------------------------
  // forceReclaim
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("forceReclaim() reverts PROCESSING to PENDING and bumps attempts + version")
  void forceReclaim_revertsToPending() {
    ClaimedEvent claimed = publishAndClaim(EVENT_TYPE_A, "x", WORKER_1);
    Instant rerun = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MICROS);

    boolean ok = store.forceReclaim(claimed.id(), WORKER_1, claimed.claimedVersion(), rerun);

    assertThat(ok).isTrue();
    Event after = store.findById(claimed.id()).orElseThrow();
    assertThat(after.status()).isEqualTo(EventStatus.PENDING);
    assertThat(after.claimedBy()).isNull();
    assertThat(after.claimedAt()).isNull();
    assertThat(after.attempts()).isEqualTo(claimed.attempts() + 1);
    assertThat(after.version()).isGreaterThan(claimed.claimedVersion());
    assertThat(after.runAt()).isEqualTo(rerun);
  }

  @Test
  @DisplayName("forceReclaim() returns false on a stale version (finalize already applied)")
  void forceReclaim_false_onStaleVersion() {
    ClaimedEvent claimed = publishAndClaim(EVENT_TYPE_A, "x", WORKER_1);

    boolean ok =
        store.forceReclaim(
            claimed.id(), WORKER_1, claimed.claimedVersion() - 1, Instant.now());

    assertThat(ok).isFalse();
  }

  @Test
  @DisplayName("finalize-vs-watchdog race: exactly one caller wins on the same version")
  void finalize_vs_watchdog_race() {
    ClaimedEvent claimed = publishAndClaim(EVENT_TYPE_A, "x", WORKER_1);

    boolean finalized = store.markProcessed(claimed.id(), WORKER_1, claimed.claimedVersion());
    boolean forced =
        store.forceReclaim(claimed.id(), WORKER_1, claimed.claimedVersion(), Instant.now());

    assertThat(finalized ^ forced).isTrue();
  }

  // ---------------------------------------------------------------------------------------------
  // reclaimOrphans
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("reclaimOrphans() reverts every PROCESSING row owned by the listed workers")
  void reclaimOrphans_revertsAllRows() {
    ClaimedEvent c1 = publishAndClaim(EVENT_TYPE_A, "1", WORKER_1);
    ClaimedEvent c2 = publishAndClaim(EVENT_TYPE_B, "2", WORKER_1);
    ClaimedEvent c3 = publishAndClaim(EVENT_TYPE_A, "3", WORKER_2);

    int n = store.reclaimOrphans(List.of(WORKER_1), Instant.now());

    assertThat(n).isEqualTo(2);
    assertThat(store.findById(c1.id()).orElseThrow().status()).isEqualTo(EventStatus.PENDING);
    assertThat(store.findById(c2.id()).orElseThrow().status()).isEqualTo(EventStatus.PENDING);
    assertThat(store.findById(c3.id()).orElseThrow().status()).isEqualTo(EventStatus.PROCESSING);
  }

  @Test
  @DisplayName("reclaimOrphans() on an empty worker list is a no-op and returns zero")
  void reclaimOrphans_emptyList_noOp() {
    publishAndClaim(EVENT_TYPE_A, "1", WORKER_1);

    assertThat(store.reclaimOrphans(List.of(), Instant.now())).isZero();
  }

  // ---------------------------------------------------------------------------------------------
  // metricsSnapshot
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("metricsSnapshot() reports totals that match the current store state")
  void metricsSnapshot_reflectsState() {
    // Build state deterministically — claim() would otherwise grab every matching PENDING row.
    // Disabled row.
    ClaimedEvent disabled = publishAndClaim(EVENT_TYPE_A, "disabled", WORKER_1);
    store.markDisabled(disabled.id(), WORKER_1, disabled.claimedVersion(), "disabled");
    // Processing row.
    publishAndClaim(EVENT_TYPE_B, "processing", WORKER_2);
    // Three pending rows, added last so no claim grabs them.
    Instant past = Instant.now().minusSeconds(1);
    store.save(pending(EVENT_TYPE_A, "p1", past));
    store.save(pending(EVENT_TYPE_A, "p2", past));
    store.save(pending(EVENT_TYPE_B, "p3", past));

    OutboxMetricsSnapshot snapshot = store.metricsSnapshot();

    assertThat(snapshot.totalPending()).isEqualTo(3);
    assertThat(snapshot.totalProcessing()).isEqualTo(1);
    assertThat(snapshot.totalDisabled()).isEqualTo(1);
    assertThat(snapshot.takenAt()).isNotNull();
  }

  // ---------------------------------------------------------------------------------------------
  // input validation
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("ClaimRequest rejects non-positive limits")
  void claimRequest_rejectsBadLimit() {
    assertThatThrownBy(() -> new ClaimRequest(EVENT_TYPE_A, WORKER_1, 0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new ClaimRequest(EVENT_TYPE_A, WORKER_1, -1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ---------------------------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------------------------

  protected PendingEvent pending(String type, String rawPayload, Instant runAt) {
    return pending(type, rawPayload, runAt, (short) 0);
  }

  protected PendingEvent pending(String type, String rawPayload, Instant runAt, short priority) {
    return PendingEvent.builder()
        .id(UUID.randomUUID())
        .eventType(type)
        .payload(jsonString(rawPayload))
        .payloadClass("java.lang.String")
        .priority(priority)
        .runAt(runAt)
        .traceContext(Map.of())
        .build();
  }

  /**
   * Encode {@code raw} as a JSON string literal so the payload is valid JSON for adapters that
   * persist it as JSONB. In-memory adapters round-trip it verbatim; PG adapters store and return
   * it as a canonical JSON scalar (no whitespace inserted).
   */
  protected static String jsonString(String raw) {
    return "\"" + raw.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }

  protected ClaimedEvent publishAndClaim(String type, String payload, WorkerId worker) {
    PendingEvent p = pending(type, payload, Instant.now().minusSeconds(1));
    store.save(p);
    List<ClaimedEvent> claimed = store.claim(new ClaimRequest(type, worker, 10));
    return claimed.stream()
        .filter(ce -> ce.id().equals(p.id()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("event not claimed: " + p.id()));
  }
}
