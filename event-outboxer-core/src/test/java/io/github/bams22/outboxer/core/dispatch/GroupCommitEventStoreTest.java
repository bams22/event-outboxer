/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.dispatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bams22.outboxer.core.support.ForwardingEventStore;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.domain.exception.EventStoreException;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Behaviour of the group-commit finalize decorator: batches form under concurrency, per-row
 * verdicts route back to the right caller, and a delegate failure surfaces to every caller of the
 * failed batch — synchronously, exactly like a direct call would.
 */
class GroupCommitEventStoreTest {

    private static final WorkerId WORKER = new WorkerId("gc-worker");

    @Test
    @DisplayName(
            "N concurrent finalizes coalesce into fewer batch statements, all verdicts correct")
    void concurrentFinalizesCoalesce() throws Exception {
        int threads = 16;
        SlowRecordingStore delegate = new SlowRecordingStore(20);
        GroupCommitEventStore store = new GroupCommitEventStore(delegate, WORKER, 128);

        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            ids.add(UUID.randomUUID());
        }
        delegate.acceptAll(ids);

        List<Boolean> results =
                runConcurrently(threads, i -> store.markProcessed(ids.get(i), WORKER, 1L));

        assertThat(results).hasSize(threads).allMatch(Boolean::booleanValue);
        assertThat(delegate.processedBatchCalls.get())
                .as("group commit must issue fewer statements than events")
                .isLessThan(threads);
        assertThat(delegate.allProcessedMarks()).containsExactlyInAnyOrderElementsOf(ids);
    }

    @Test
    @DisplayName("per-row race verdicts: losers get false while winners in the same batch get true")
    void perRowVerdictsRouteToCallers() throws Exception {
        UUID winner = UUID.randomUUID();
        UUID loser = UUID.randomUUID();
        SlowRecordingStore delegate = new SlowRecordingStore(20);
        delegate.acceptAll(List.of(winner));
        GroupCommitEventStore store = new GroupCommitEventStore(delegate, WORKER, 128);

        List<UUID> ids = List.of(winner, loser);
        List<Boolean> results =
                runConcurrently(2, i -> store.markProcessed(ids.get(i), WORKER, 1L));

        assertThat(results).containsExactly(true, false);
    }

    @Test
    @DisplayName("a delegate failure surfaces to every caller of the failed batch")
    void delegateFailurePropagatesToAllCallers() throws Exception {
        SlowRecordingStore delegate = new SlowRecordingStore(20);
        delegate.failProcessed = true;
        GroupCommitEventStore store = new GroupCommitEventStore(delegate, WORKER, 128);

        ExecutorService exec = Executors.newFixedThreadPool(4);
        try {
            List<Future<Boolean>> futures = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                UUID id = UUID.randomUUID();
                futures.add(exec.submit(() -> store.markProcessed(id, WORKER, 1L)));
            }
            for (Future<Boolean> f : futures) {
                assertThatThrownBy(f::get).cause().isInstanceOf(EventStoreException.class);
            }
        } finally {
            exec.shutdownNow();
        }
    }

    @Test
    @DisplayName("idle engine: a single finalize flushes immediately as a batch of one")
    void singleCallFlushesImmediately() {
        SlowRecordingStore delegate = new SlowRecordingStore(0);
        UUID id = UUID.randomUUID();
        delegate.acceptAll(List.of(id));
        GroupCommitEventStore store = new GroupCommitEventStore(delegate, WORKER, 128);

        assertThat(store.markProcessed(id, WORKER, 1L)).isTrue();

        assertThat(delegate.processedBatchCalls.get()).isEqualTo(1);
        assertThat(delegate.processedBatchSizes).containsExactly(1);
    }

    @Test
    @DisplayName("processed and retry marks flush through their own batch statements")
    void mixedKindsFlushSeparately() throws Exception {
        SlowRecordingStore delegate = new SlowRecordingStore(20);
        GroupCommitEventStore store = new GroupCommitEventStore(delegate, WORKER, 128);
        UUID processedId = UUID.randomUUID();
        UUID retryId = UUID.randomUUID();
        delegate.acceptAll(List.of(processedId, retryId));

        List<Boolean> results =
                runConcurrently(
                        2,
                        i ->
                                i == 0
                                        ? store.markProcessed(processedId, WORKER, 1L)
                                        : store.markForRetry(
                                                retryId, WORKER, 1L, "boom", Instant.now()));

        assertThat(results).containsExactly(true, true);
        assertThat(delegate.allProcessedMarks()).containsExactly(processedId);
        assertThat(delegate.allRetryMarks()).containsExactly(retryId);
    }

    @Test
    @DisplayName("maxBatchSize caps a single statement; all entries still flush")
    void maxBatchSizeCapsStatementSize() throws Exception {
        int threads = 12;
        SlowRecordingStore delegate = new SlowRecordingStore(30);
        GroupCommitEventStore store = new GroupCommitEventStore(delegate, WORKER, 4);
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            ids.add(UUID.randomUUID());
        }
        delegate.acceptAll(ids);

        List<Boolean> results =
                runConcurrently(threads, i -> store.markProcessed(ids.get(i), WORKER, 1L));

        assertThat(results).allMatch(Boolean::booleanValue);
        assertThat(delegate.processedBatchSizes).allMatch(size -> size <= 4);
        assertThat(delegate.allProcessedMarks()).containsExactlyInAnyOrderElementsOf(ids);
    }

    @Test
    @DisplayName("maxBatchSize must be positive")
    void rejectsNonPositiveMaxBatchSize() {
        assertThatThrownBy(() -> new GroupCommitEventStore(new InMemoryEventStore(), WORKER, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("a foreign worker id is rejected — the decorator is bound to the engine's worker")
    void rejectsForeignWorker() {
        GroupCommitEventStore store =
                new GroupCommitEventStore(new InMemoryEventStore(), WORKER, 128);

        assertThatThrownBy(() -> store.markProcessed(UUID.randomUUID(), new WorkerId("other"), 1L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    private interface IndexedCall {
        boolean run(int index);
    }

    /**
     * Runs one call per thread, released simultaneously by a latch, and returns the results in
     * submission order.
     */
    private static List<Boolean> runConcurrently(int threads, IndexedCall call) throws Exception {
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Boolean>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                int index = i;
                futures.add(
                        exec.submit(
                                (Callable<Boolean>)
                                        () -> {
                                            start.await();
                                            return call.run(index);
                                        }));
            }
            start.countDown();
            List<Boolean> results = new ArrayList<>();
            for (Future<Boolean> f : futures) {
                results.add(f.get(10, TimeUnit.SECONDS));
            }
            return results;
        } finally {
            exec.shutdownNow();
        }
    }

    /**
     * Delegate double: batch calls sleep to widen the group-commit window, record batch sizes and
     * apply marks against a preset accept-set (simulating per-row optimistic-lock verdicts).
     */
    private static final class SlowRecordingStore extends ForwardingEventStore {

        final AtomicInteger processedBatchCalls = new AtomicInteger();
        final ConcurrentLinkedQueue<Integer> processedBatchSizes = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<UUID> processedMarks = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<UUID> retryMarks = new ConcurrentLinkedQueue<>();
        private final Set<UUID> accepted = java.util.concurrent.ConcurrentHashMap.newKeySet();
        private final long sleepMillis;
        volatile boolean failProcessed;

        SlowRecordingStore(long sleepMillis) {
            super(new InMemoryEventStore());
            this.sleepMillis = sleepMillis;
        }

        void acceptAll(List<UUID> ids) {
            accepted.addAll(ids);
        }

        List<UUID> allProcessedMarks() {
            return List.copyOf(processedMarks);
        }

        List<UUID> allRetryMarks() {
            return List.copyOf(retryMarks);
        }

        @Override
        public Set<UUID> markProcessedAll(List<EventStore.ProcessedMark> marks, WorkerId workerId) {
            processedBatchCalls.incrementAndGet();
            processedBatchSizes.add(marks.size());
            sleep();
            if (failProcessed) {
                throw new EventStoreException("batch finalize failed");
            }
            Set<UUID> applied = java.util.concurrent.ConcurrentHashMap.newKeySet();
            for (EventStore.ProcessedMark mark : marks) {
                processedMarks.add(mark.id());
                if (accepted.contains(mark.id())) {
                    applied.add(mark.id());
                }
            }
            return applied;
        }

        @Override
        public Set<UUID> markForRetryAll(List<EventStore.RetryMark> marks, WorkerId workerId) {
            sleep();
            Set<UUID> applied = java.util.concurrent.ConcurrentHashMap.newKeySet();
            for (EventStore.RetryMark mark : marks) {
                retryMarks.add(mark.id());
                if (accepted.contains(mark.id())) {
                    applied.add(mark.id());
                }
            }
            return applied;
        }

        private void sleep() {
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
