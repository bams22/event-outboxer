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
import io.github.bams22.outboxer.domain.CoalescedEvent;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.OutboxMetricsSnapshot;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Reusable contract specification for every {@link EventStore} implementation. Subclasses provide a
 * fresh store via {@link #newStore()}; each test executes against its own isolated store so that
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
    protected static final String TEST_FORMAT = "test-json";

    protected EventStore store;

    /** Build a fresh {@link EventStore} backed by whatever state the adapter needs. */
    protected abstract EventStore newStore();

    /**
     * Force the stored {@code claimed_at} of a PROCESSING row to the (past) instant {@code at}.
     * Adapters stamp claims with their own time source, so the staleness scenarios of {@code
     * sweepStale} have to inject age through a direct write.
     */
    protected abstract void backdateClaim(UUID id, Instant at);

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
        assertThat(event.payload()).isEqualTo(SerializedPayload.ofText(jsonString("hello")));
        assertThat(event.payloadFormat()).isEqualTo(TEST_FORMAT);
        assertThat(event.status()).isEqualTo(EventStatus.PENDING);
        assertThat(event.attempts()).isZero();
        assertThat(event.version()).isGreaterThanOrEqualTo(0L);
        assertThat(event.claimedBy()).isNull();
        assertThat(event.claimedAt()).isNull();
        assertThat(event.lastFailReason()).isNull();
    }

    @Test
    @DisplayName("save() round-trips a binary payload byte-exact through claim and findById")
    void save_binaryPayload_roundTripsByteExact() {
        byte[] raw = binaryPayloadFixture();
        PendingEvent pending = pendingBinary(EVENT_TYPE_A, raw, Instant.now().minusSeconds(1));

        store.save(pending);

        Event event = store.findById(pending.id()).orElseThrow();
        assertThat(event.payload().isText()).isFalse();
        assertThat(event.payload().requireBytes()).isEqualTo(raw);
        assertThat(event.payloadFormat()).isEqualTo("test-binary");

        ClaimedEvent claimed = claimOneById(EVENT_TYPE_A, pending.id());
        assertThat(claimed.payload().requireBytes()).isEqualTo(raw);
        assertThat(claimed.payloadFormat()).isEqualTo("test-binary");
    }

    @Test
    @DisplayName("claim() preserves the payload format written at publish time")
    void save_textPayload_preservesFormat() {
        PendingEvent pending = pending(EVENT_TYPE_A, "keep-format", Instant.now().minusSeconds(1));

        store.save(pending);

        ClaimedEvent claimed = claimOneById(EVENT_TYPE_A, pending.id());
        assertThat(claimed.payloadFormat()).isEqualTo(TEST_FORMAT);
        assertThat(claimed.payload())
                .isEqualTo(SerializedPayload.ofText(jsonString("keep-format")));
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
                .containsExactly(
                        SerializedPayload.ofText(jsonString("hi-old")),
                        SerializedPayload.ofText(jsonString("hi-new")),
                        SerializedPayload.ofText(jsonString("lo-old")));
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
    @DisplayName(
            "claim() transitions rows to PROCESSING and records claimedBy / claimedAt / version")
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
            List<Future<?>> futures = new ArrayList<>();
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
                                        List<ClaimedEvent> batch =
                                                store.claim(new ClaimRequest(EVENT_TYPE_A, wid, 7));
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
    // markProcessedAll / markForRetryAll (batch finalize, ADR-0014 batch form)
    // ---------------------------------------------------------------------------------------------

    @Test
    @DisplayName("markProcessedAll() keeps per-row guards: winners removed, stale row untouched")
    void markProcessedAll_perRowGuards() {
        ClaimedEvent a = publishAndClaim(EVENT_TYPE_A, "a", WORKER_1);
        ClaimedEvent b = publishAndClaim(EVENT_TYPE_A, "b", WORKER_1);
        ClaimedEvent c = publishAndClaim(EVENT_TYPE_B, "c", WORKER_1);

        Set<UUID> applied =
                store.markProcessedAll(
                        List.of(
                                new EventStore.ProcessedMark(a.id(), a.claimedVersion()),
                                new EventStore.ProcessedMark(
                                        b.id(), b.claimedVersion() - 1), // stale
                                new EventStore.ProcessedMark(c.id(), c.claimedVersion())),
                        WORKER_1);

        assertThat(applied).containsExactlyInAnyOrder(a.id(), c.id());
        assertThat(store.findById(a.id())).isEmpty();
        assertThat(store.findById(b.id())).isPresent();
        assertThat(store.findById(c.id())).isEmpty();
    }

    @Test
    @DisplayName("markProcessedAll() applies nothing for a different worker")
    void markProcessedAll_empty_onDifferentWorker() {
        ClaimedEvent a = publishAndClaim(EVENT_TYPE_A, "a", WORKER_1);
        ClaimedEvent b = publishAndClaim(EVENT_TYPE_A, "b", WORKER_1);

        Set<UUID> applied =
                store.markProcessedAll(
                        List.of(
                                new EventStore.ProcessedMark(a.id(), a.claimedVersion()),
                                new EventStore.ProcessedMark(b.id(), b.claimedVersion())),
                        WORKER_2);

        assertThat(applied).isEmpty();
        assertThat(store.findById(a.id())).isPresent();
        assertThat(store.findById(b.id())).isPresent();
    }

    @Test
    @DisplayName("markProcessedAll() of an empty list returns an empty set")
    void markProcessedAll_emptyInput() {
        assertThat(store.markProcessedAll(List.of(), WORKER_1)).isEmpty();
    }

    @Test
    @DisplayName("markForRetryAll() applies per-row reason and runAt, bumps attempts and version")
    void markForRetryAll_perRowFields() {
        ClaimedEvent a = publishAndClaim(EVENT_TYPE_A, "a", WORKER_1);
        ClaimedEvent b = publishAndClaim(EVENT_TYPE_A, "b", WORKER_1);
        ClaimedEvent stale = publishAndClaim(EVENT_TYPE_B, "s", WORKER_1);
        // Truncate to microseconds: TIMESTAMPTZ in PG is microsecond-precision.
        Instant runA = Instant.now().plusSeconds(30).truncatedTo(ChronoUnit.MICROS);
        Instant runB = Instant.now().plusSeconds(90).truncatedTo(ChronoUnit.MICROS);

        Set<UUID> applied =
                store.markForRetryAll(
                        List.of(
                                new EventStore.RetryMark(
                                        a.id(), a.claimedVersion(), "boom-a", runA),
                                new EventStore.RetryMark(
                                        b.id(), b.claimedVersion(), "boom-b", runB),
                                new EventStore.RetryMark(
                                        stale.id(), stale.claimedVersion() - 1, "stale", runA)),
                        WORKER_1);

        assertThat(applied).containsExactlyInAnyOrder(a.id(), b.id());
        Event afterA = store.findById(a.id()).orElseThrow();
        assertThat(afterA.status()).isEqualTo(EventStatus.PENDING);
        assertThat(afterA.claimedBy()).isNull();
        assertThat(afterA.attempts()).isEqualTo(a.attempts() + 1);
        assertThat(afterA.version()).isGreaterThan(a.claimedVersion());
        assertThat(afterA.lastFailReason()).isEqualTo("boom-a");
        assertThat(afterA.runAt()).isEqualTo(runA);
        Event afterB = store.findById(b.id()).orElseThrow();
        assertThat(afterB.lastFailReason()).isEqualTo("boom-b");
        assertThat(afterB.runAt()).isEqualTo(runB);
        Event afterStale = store.findById(stale.id()).orElseThrow();
        assertThat(afterStale.status()).isEqualTo(EventStatus.PROCESSING);
        assertThat(afterStale.attempts()).isEqualTo(stale.attempts());
    }

    @Test
    @DisplayName("markForRetryAll() of an empty list returns an empty set")
    void markForRetryAll_emptyInput() {
        assertThat(store.markForRetryAll(List.of(), WORKER_1)).isEmpty();
    }

    // ---------------------------------------------------------------------------------------------
    // markForRetry
    // ---------------------------------------------------------------------------------------------

    @Test
    @DisplayName("markForRetry() reverts to PENDING, clears claim, bumps attempts and version")
    void markForRetry_revertsToPending() {
        ClaimedEvent claimed = publishAndClaim(EVENT_TYPE_A, "x", WORKER_1);
        // Truncate to microseconds: TIMESTAMPTZ in PG is microsecond-precision.
        Instant nextRun = Instant.now().plusSeconds(60).truncatedTo(ChronoUnit.MICROS);

        boolean ok =
                store.markForRetry(
                        claimed.id(), WORKER_1, claimed.claimedVersion(), "transient", nextRun);

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
        Instant rerun = Instant.now().truncatedTo(ChronoUnit.MICROS);

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
    // dedup key coalescing at claim time (ADR-0037)
    // ---------------------------------------------------------------------------------------------

    @Test
    @DisplayName("keyed events always insert: the key never blocks a save or a saveAll")
    void save_keyedEventsAlwaysInsert() {
        PendingEvent first = pendingWithKey(EVENT_TYPE_A, "p1", "order-1");
        PendingEvent second = pendingWithKey(EVENT_TYPE_A, "p2", "order-1");
        store.save(first);
        store.save(second);
        PendingEvent third = pendingWithKey(EVENT_TYPE_A, "p3", "order-1");
        PendingEvent fourth = pendingWithKey(EVENT_TYPE_B, "p4", "order-1");
        store.saveAll(List.of(third, fourth));

        for (PendingEvent p : List.of(first, second, third, fourth)) {
            assertThat(store.findById(p.id()).orElseThrow().status())
                    .isEqualTo(EventStatus.PENDING);
        }
    }

    @Test
    @DisplayName("claim keeps the oldest due row of a key and sweeps the others, in the batch")
    void claim_collapsesDueDuplicatesOfAKey() {
        Instant base = Instant.now().minusSeconds(30);
        PendingEvent oldest = pendingWithKey(EVENT_TYPE_A, "v1", "order-9", base);
        PendingEvent middle = pendingWithKey(EVENT_TYPE_A, "v2", "order-9", base.plusSeconds(1));
        PendingEvent newest = pendingWithKey(EVENT_TYPE_A, "v3", "order-9", base.plusSeconds(2));
        PendingEvent otherKey = pendingWithKey(EVENT_TYPE_A, "o", "order-10", base);
        PendingEvent keyless = pending(EVENT_TYPE_A, "no-key", base);
        for (PendingEvent p : List.of(oldest, middle, newest, otherKey, keyless)) {
            store.save(p);
        }

        List<ClaimedEvent> claimed = store.claim(new ClaimRequest(EVENT_TYPE_A, WORKER_1, 10));

        assertThat(claimed)
                .extracting(ClaimedEvent::id)
                .containsExactlyInAnyOrder(oldest.id(), otherKey.id(), keyless.id());
        ClaimedEvent representative =
                claimed.stream().filter(c -> c.id().equals(oldest.id())).findFirst().orElseThrow();
        assertThat(representative.dedupKey()).isEqualTo("order-9");
        assertThat(representative.coalesced())
                .extracting(CoalescedEvent::id)
                .containsExactlyInAnyOrder(middle.id(), newest.id());
        assertThat(store.findById(middle.id())).isEmpty();
        assertThat(store.findById(newest.id())).isEmpty();
        claimed.stream()
                .filter(c -> !c.id().equals(oldest.id()))
                .forEach(c -> assertThat(c.coalesced()).isEmpty());
    }

    @Test
    @DisplayName("duplicates beyond the claim batch are swept by the same claim")
    void claim_sweepsDuplicatesBeyondTheBatch() {
        Instant base = Instant.now().minusSeconds(60);
        PendingEvent first = pendingWithKey(EVENT_TYPE_A, "v0", "order-7", base);
        store.save(first);
        for (int i = 1; i < 30; i++) {
            store.save(pendingWithKey(EVENT_TYPE_A, "v" + i, "order-7", base.plusSeconds(i)));
        }

        List<ClaimedEvent> claimed = store.claim(new ClaimRequest(EVENT_TYPE_A, WORKER_1, 5));

        assertThat(claimed)
                .singleElement()
                .satisfies(c -> assertThat(c.id()).isEqualTo(first.id()));
        assertThat(claimed.getFirst().coalesced()).hasSize(29);
        assertThat(store.claim(new ClaimRequest(EVENT_TYPE_A, WORKER_1, 100))).isEmpty();
    }

    @Test
    @DisplayName("the swept duplicates' trace contexts travel with the representative")
    void claim_carriesTheSweptTraceContexts() {
        Instant base = Instant.now().minusSeconds(30);
        store.save(pendingWithKey(EVENT_TYPE_A, "v1", "order-3", base));
        PendingEvent twin =
                pendingWithKeyAt(
                        EVENT_TYPE_A,
                        "v2",
                        "order-3",
                        base.plusSeconds(1),
                        Map.of("traceparent", "00-abc-def-01"));
        store.save(twin);

        ClaimedEvent representative =
                store.claim(new ClaimRequest(EVENT_TYPE_A, WORKER_1, 10)).getFirst();

        assertThat(representative.coalesced())
                .singleElement()
                .satisfies(
                        c -> {
                            assertThat(c.id()).isEqualTo(twin.id());
                            assertThat(c.traceContext())
                                    .containsEntry("traceparent", "00-abc-def-01");
                        });
    }

    @Test
    @DisplayName("a deferred twin (future runAt) is a separate intent and is never swept")
    void claim_keepsDeferredTwin() {
        PendingEvent due = pendingWithKey(EVENT_TYPE_A, "now", "order-5");
        PendingEvent later =
                pendingWithKey(EVENT_TYPE_A, "later", "order-5", Instant.now().plusSeconds(3600));
        store.save(due);
        store.save(later);

        List<ClaimedEvent> claimed = store.claim(new ClaimRequest(EVENT_TYPE_A, WORKER_1, 10));

        assertThat(claimed).singleElement().satisfies(c -> assertThat(c.id()).isEqualTo(due.id()));
        assertThat(claimed.getFirst().coalesced()).isEmpty();
        assertThat(store.findById(later.id()).orElseThrow().status())
                .isEqualTo(EventStatus.PENDING);
    }

    @Test
    @DisplayName("keys are scoped per event type: the same key in two types never collapses")
    void claim_doesNotCollapseAcrossTypes() {
        store.save(pendingWithKey(EVENT_TYPE_A, "a", "shared"));
        store.save(pendingWithKey(EVENT_TYPE_B, "b", "shared"));

        assertThat(store.claim(new ClaimRequest(EVENT_TYPE_A, WORKER_1, 10)))
                .singleElement()
                .satisfies(c -> assertThat(c.coalesced()).isEmpty());
        assertThat(store.claim(new ClaimRequest(EVENT_TYPE_B, WORKER_1, 10)))
                .singleElement()
                .satisfies(c -> assertThat(c.coalesced()).isEmpty());
    }

    @Test
    @DisplayName("after the representative is processed the key is free again — not exactly-once")
    void claim_keyFreeAfterProcessing() {
        PendingEvent first = pendingWithKey(EVENT_TYPE_A, "v1", "order-5");
        store.save(first);
        ClaimedEvent claimed = claimOneById(EVENT_TYPE_A, first.id());
        assertThat(store.markProcessed(claimed.id(), WORKER_1, claimed.claimedVersion())).isTrue();

        PendingEvent again = pendingWithKey(EVENT_TYPE_A, "v2", "order-5");
        store.save(again);
        assertThat(claimOneById(EVENT_TYPE_A, again.id()).coalesced()).isEmpty();
    }

    // The regression of ADR-0037: with a twin inserted while the key's event is PROCESSING, every
    // way back to PENDING used to collide with the V004 unique index (23505). Now the twin is a
    // plain row and the next claim collapses the two.

    @Test
    @DisplayName("markForRetry with a PENDING twin of the key succeeds; the next claim collapses")
    void markForRetry_withTwin_noLongerCollides() {
        ClaimedEvent first = claimKeyed("order-11", Instant.now().minusSeconds(20));
        PendingEvent twin = pendingWithKey(EVENT_TYPE_A, "twin", "order-11");
        store.save(twin);

        assertThat(
                        store.markForRetry(
                                first.id(),
                                WORKER_1,
                                first.claimedVersion(),
                                "boom",
                                Instant.now().minusSeconds(10)))
                .isTrue();

        assertCollapsesToRepresentative(first.id(), twin.id());
    }

    @Test
    @DisplayName("release with a PENDING twin of the key succeeds; the next claim collapses")
    void release_withTwin_noLongerCollides() {
        ClaimedEvent first = claimKeyed("order-12", Instant.now().minusSeconds(20));
        PendingEvent twin = pendingWithKey(EVENT_TYPE_A, "twin", "order-12");
        store.save(twin);

        assertThat(
                        store.release(
                                first.id(),
                                WORKER_1,
                                first.claimedVersion(),
                                "lock busy",
                                Instant.now().minusSeconds(10)))
                .isTrue();

        assertCollapsesToRepresentative(first.id(), twin.id());
    }

    @Test
    @DisplayName("forceReclaim with a PENDING twin of the key succeeds; the next claim collapses")
    void forceReclaim_withTwin_noLongerCollides() {
        ClaimedEvent first = claimKeyed("order-13", Instant.now().minusSeconds(20));
        PendingEvent twin = pendingWithKey(EVENT_TYPE_A, "twin", "order-13");
        store.save(twin);

        assertThat(
                        store.forceReclaim(
                                first.id(),
                                WORKER_1,
                                first.claimedVersion(),
                                Instant.now().minusSeconds(10)))
                .isTrue();

        assertCollapsesToRepresentative(first.id(), twin.id());
    }

    @Test
    @DisplayName("reclaimOrphans with a PENDING twin of the key succeeds; the next claim collapses")
    void reclaimOrphans_withTwin_noLongerCollides() {
        ClaimedEvent first = claimKeyed("order-14", Instant.now().minusSeconds(20));
        PendingEvent twin = pendingWithKey(EVENT_TYPE_A, "twin", "order-14");
        store.save(twin);

        assertThat(store.reclaimOrphans(List.of(WORKER_1), Instant.now().minusSeconds(10)))
                .isEqualTo(1);

        assertCollapsesToRepresentative(first.id(), twin.id());
    }

    private ClaimedEvent claimKeyed(String key, Instant runAt) {
        PendingEvent p = pendingWithKey(EVENT_TYPE_A, "first", key, runAt);
        store.save(p);
        return claimOneById(EVENT_TYPE_A, p.id());
    }

    /**
     * Both rows are PENDING now; one claim keeps the older {@code representative} and sweeps {@code
     * swept}.
     */
    private void assertCollapsesToRepresentative(UUID representative, UUID swept) {
        assertThat(store.findById(representative).orElseThrow().status())
                .isEqualTo(EventStatus.PENDING);
        assertThat(store.findById(swept).orElseThrow().status()).isEqualTo(EventStatus.PENDING);

        List<ClaimedEvent> claimed = store.claim(new ClaimRequest(EVENT_TYPE_A, WORKER_1, 10));

        assertThat(claimed)
                .singleElement()
                .satisfies(
                        c -> {
                            assertThat(c.id()).isEqualTo(representative);
                            assertThat(c.coalesced())
                                    .extracting(CoalescedEvent::id)
                                    .containsExactly(swept);
                        });
        assertThat(store.findById(swept)).isEmpty();
    }

    // ---------------------------------------------------------------------------------------------
    // release / releaseClaimed
    // ---------------------------------------------------------------------------------------------

    @Test
    @DisplayName("release() reverts to PENDING and bumps version WITHOUT incrementing attempts")
    void release_revertsToPending_withoutAttemptsBump() {
        ClaimedEvent claimed = publishAndClaim(EVENT_TYPE_A, "x", WORKER_1);
        Instant nextRun = Instant.now().plusSeconds(30).truncatedTo(ChronoUnit.MICROS);

        boolean ok =
                store.release(
                        claimed.id(), WORKER_1, claimed.claimedVersion(), "lock busy", nextRun);

        assertThat(ok).isTrue();
        Event after = store.findById(claimed.id()).orElseThrow();
        assertThat(after.status()).isEqualTo(EventStatus.PENDING);
        assertThat(after.claimedBy()).isNull();
        assertThat(after.claimedAt()).isNull();
        assertThat(after.attempts()).isEqualTo(claimed.attempts());
        assertThat(after.version()).isGreaterThan(claimed.claimedVersion());
        assertThat(after.lastFailReason()).isEqualTo("lock busy");
        assertThat(after.runAt()).isEqualTo(nextRun);
    }

    @Test
    @DisplayName("release() returns false on a stale version")
    void release_false_onStaleVersion() {
        ClaimedEvent claimed = publishAndClaim(EVENT_TYPE_A, "x", WORKER_1);

        boolean ok =
                store.release(
                        claimed.id(),
                        WORKER_1,
                        claimed.claimedVersion() - 1,
                        "no-op",
                        Instant.now().plusSeconds(10));

        assertThat(ok).isFalse();
        assertThat(store.findById(claimed.id()).orElseThrow().status())
                .isEqualTo(EventStatus.PROCESSING);
    }

    @Test
    @DisplayName("release() returns false when the caller is a different worker")
    void release_false_onDifferentWorker() {
        ClaimedEvent claimed = publishAndClaim(EVENT_TYPE_A, "x", WORKER_1);

        boolean ok =
                store.release(
                        claimed.id(), WORKER_2, claimed.claimedVersion(), "no-op", Instant.now());

        assertThat(ok).isFalse();
    }

    @Test
    @DisplayName(
            "releaseClaimed() reverts every PROCESSING row of the worker without touching attempts")
    void releaseClaimed_revertsAllOwnRows() {
        ClaimedEvent c1 = publishAndClaim(EVENT_TYPE_A, "1", WORKER_1);
        ClaimedEvent c2 = publishAndClaim(EVENT_TYPE_B, "2", WORKER_1);
        ClaimedEvent other = publishAndClaim(EVENT_TYPE_A, "3", WORKER_2);
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);

        int released = store.releaseClaimed(WORKER_1, now);

        assertThat(released).isEqualTo(2);
        for (ClaimedEvent ce : List.of(c1, c2)) {
            Event after = store.findById(ce.id()).orElseThrow();
            assertThat(after.status()).isEqualTo(EventStatus.PENDING);
            assertThat(after.claimedBy()).isNull();
            assertThat(after.attempts()).isEqualTo(ce.attempts());
            assertThat(after.version()).isGreaterThan(ce.claimedVersion());
        }
        assertThat(store.findById(other.id()).orElseThrow().status())
                .isEqualTo(EventStatus.PROCESSING);
    }

    @Test
    @DisplayName("releaseClaimed() makes a late finalize from the old claim lose the race")
    void releaseClaimed_defeatsLateFinalize() {
        ClaimedEvent claimed = publishAndClaim(EVENT_TYPE_A, "x", WORKER_1);

        assertThat(store.releaseClaimed(WORKER_1, Instant.now())).isEqualTo(1);

        boolean lateFinalize =
                store.markProcessed(claimed.id(), WORKER_1, claimed.claimedVersion());
        assertThat(lateFinalize).isFalse();
        assertThat(store.findById(claimed.id()).orElseThrow().status())
                .isEqualTo(EventStatus.PENDING);
    }

    // ---------------------------------------------------------------------------------------------
    // sweepStale
    // ---------------------------------------------------------------------------------------------

    @Test
    @DisplayName("sweepStale() reclaims only PROCESSING rows older than the threshold")
    void sweepStale_reclaimsOnlyStaleRows() {
        ClaimedEvent stale = publishAndClaim(EVENT_TYPE_A, "stale", WORKER_1);
        ClaimedEvent fresh = publishAndClaim(EVENT_TYPE_A, "fresh", WORKER_1);
        backdateClaim(stale.id(), Instant.now().minus(Duration.ofHours(1)));

        int swept = store.sweepStale(Duration.ofMinutes(30), 100);

        assertThat(swept).isEqualTo(1);
        Event after = store.findById(stale.id()).orElseThrow();
        assertThat(after.status()).isEqualTo(EventStatus.PENDING);
        assertThat(after.claimedBy()).isNull();
        assertThat(after.attempts()).isEqualTo(stale.attempts() + 1);
        assertThat(after.version()).isGreaterThan(stale.claimedVersion());
        assertThat(store.findById(fresh.id()).orElseThrow().status())
                .isEqualTo(EventStatus.PROCESSING);
    }

    @Test
    @DisplayName("sweepStale() honours the limit")
    void sweepStale_respectsLimit() {
        for (int i = 0; i < 3; i++) {
            ClaimedEvent c = publishAndClaim(EVENT_TYPE_A, "s-" + i, WORKER_1);
            backdateClaim(c.id(), Instant.now().minus(Duration.ofHours(1)));
        }

        assertThat(store.sweepStale(Duration.ofMinutes(30), 2)).isEqualTo(2);
        assertThat(store.sweepStale(Duration.ofMinutes(30), 100)).isEqualTo(1);
    }

    @Test
    @DisplayName("a late finalize from the swept claim loses the optimistic-lock race")
    void sweepStale_defeatsLateFinalize() {
        ClaimedEvent stale = publishAndClaim(EVENT_TYPE_A, "stuck", WORKER_1);
        backdateClaim(stale.id(), Instant.now().minus(Duration.ofHours(1)));

        assertThat(store.sweepStale(Duration.ofMinutes(30), 100)).isEqualTo(1);

        assertThat(store.markProcessed(stale.id(), WORKER_1, stale.claimedVersion())).isFalse();
        assertThat(store.findById(stale.id()).orElseThrow().status())
                .isEqualTo(EventStatus.PENDING);
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
        assertThat(store.findById(c3.id()).orElseThrow().status())
                .isEqualTo(EventStatus.PROCESSING);
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
                .payload(SerializedPayload.ofText(jsonString(rawPayload)))
                .payloadFormat(TEST_FORMAT)
                .payloadClass("java.lang.String")
                .priority(priority)
                .runAt(runAt)
                .traceContext(Map.of())
                .build();
    }

    /**
     * A pending event whose payload travels in the binary lane (ADR-0025). The bytes deliberately
     * start with {@code 0x00 0xFF} — an invalid UTF-8 sequence — so any adapter that squeezes the
     * binary lane through a text codepath fails loudly.
     */
    protected PendingEvent pendingBinary(String type, byte[] payload, Instant runAt) {
        return PendingEvent.builder()
                .id(UUID.randomUUID())
                .eventType(type)
                .payload(SerializedPayload.ofBytes(payload))
                .payloadFormat("test-binary")
                .payloadClass("java.lang.String")
                .priority((short) 0)
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

    /**
     * Bytes that are provably not UTF-8 text and not JSON: an invalid UTF-8 prefix followed by the
     * full unsigned byte range. Byte-exact round-trips of this fixture prove the adapter stores the
     * binary lane verbatim.
     */
    protected static byte[] binaryPayloadFixture() {
        byte[] raw = new byte[258];
        raw[0] = 0x00;
        raw[1] = (byte) 0xFF;
        for (int i = 0; i < 256; i++) {
            raw[i + 2] = (byte) i;
        }
        return raw;
    }

    protected PendingEvent pendingWithKey(String type, String rawPayload, String dedupKey) {
        return pendingWithKey(type, rawPayload, dedupKey, Instant.now().minusSeconds(1));
    }

    protected PendingEvent pendingWithKey(
            String type, String rawPayload, String dedupKey, Instant runAt) {
        return pendingWithKeyAt(type, rawPayload, dedupKey, runAt);
    }

    private PendingEvent pendingWithKeyAt(
            String type, String rawPayload, String dedupKey, Instant runAt) {
        return pendingWithKeyAt(type, rawPayload, dedupKey, runAt, Map.of());
    }

    private PendingEvent pendingWithKeyAt(
            String type,
            String rawPayload,
            String dedupKey,
            Instant runAt,
            Map<String, String> traceContext) {
        return PendingEvent.builder()
                .id(UUID.randomUUID())
                .eventType(type)
                .payload(SerializedPayload.ofText(jsonString(rawPayload)))
                .payloadFormat(TEST_FORMAT)
                .payloadClass("java.lang.String")
                .priority((short) 0)
                .runAt(runAt)
                .traceContext(traceContext)
                .dedupKey(dedupKey)
                .build();
    }

    protected ClaimedEvent claimOneById(String type, UUID id) {
        return store.claim(new ClaimRequest(type, WORKER_1, 100)).stream()
                .filter(ce -> ce.id().equals(id))
                .findFirst()
                .orElseThrow(() -> new AssertionError("event not claimed: " + id));
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
