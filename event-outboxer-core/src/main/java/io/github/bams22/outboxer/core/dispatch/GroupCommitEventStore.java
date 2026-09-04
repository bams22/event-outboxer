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

import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.OutboxMetricsSnapshot;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * {@link EventStore} decorator that batches {@link #markProcessed} and {@link #markForRetry} calls
 * into multi-row statements via <em>group commit</em> — the same idea databases use to amortize WAL
 * fsyncs.
 *
 * <p>A finalizing handler thread enqueues its mark and <em>tries</em> the flush lock. If the lock
 * is free (idle engine), the thread flushes everything queued — its own single entry, typically —
 * immediately: behaviour and latency are identical to a direct call. Under load, while the current
 * lock owner's batch statement is in flight, other handler threads enqueue and wait <em>on their
 * own future</em>, never on the lock; the owner's drain picks them all up into one {@link
 * EventStore#markProcessedAll} / {@link EventStore#markForRetryAll} statement and completes their
 * futures. The batch forms naturally out of the SQL round-trip time — no timers, no dedicated
 * flusher thread, no added latency floor.
 *
 * <p><b>Hand-off.</b> An owner flushes exactly once per lock acquisition (bounded duty: its own
 * event's entity lock and in-flight slot stay held only for that one statement) and then releases.
 * Entries enqueued after its drain are still queued; if the owner leaves the queues non-empty it
 * completes the current {@code flusherWanted} generation, which wakes every waiter; one of them
 * wins {@code tryLock} and flushes, the others go back to waiting on their futures and on the next
 * generation. Waiters never block on the lock: the 2026-09-04 amendment of ADR-0014 records the
 * convoy the previous {@code lock()}-then-check design formed under commit-bound round trips —
 * parked threads whose entries were already flushed kept losing the unfair lock to newly arriving
 * threads, each of which ran a full round trip while they waited.
 *
 * <p>The call stays <strong>synchronous</strong>, which is what preserves the dispatcher's
 * invariants untouched: finalize still completes before the entity lock is closed and before the
 * event leaves the in-flight registry, listeners still fire per event on the handler thread, and a
 * storage failure still surfaces to the caller (each drained entry's future is completed
 * exceptionally, and the dispatcher's finalize-failure release handles it per event). At shutdown
 * no extra draining is needed: once the handler executors have drained, every finalize has already
 * returned.
 *
 * <p>Correctness sketch (liveness): an entry {@code E} is enqueued at time {@code t}, before its
 * thread tries the lock. If the thread wins the lock it drains {@code E} itself. If it loses, some
 * owner {@code O} holds the lock; either {@code O}'s drain happened after {@code t} and included
 * {@code E}, or it happened before {@code t} — then {@code O} unlocks and checks the queues
 * <em>after</em> unlocking, finds {@code E}, and completes the generation that {@code E}'s thread
 * snapshotted before it started waiting (the snapshot is taken after the failed {@code tryLock},
 * hence before {@code O}'s post-unlock signal). A thread that snapshots a generation and then sees
 * the lock free does not wait but retries the lock, which closes the window between {@code O}'s
 * unlock and its signal. Every drained entry's future is always completed, exceptionally on a
 * delegate failure, so the final {@code join()} never blocks.
 *
 * <p>Only {@code markProcessed} (Success, Skip and Delete outcomes all route through it) and {@code
 * markForRetry} are batched — the queues are engine-wide, so finalizations of different event types
 * coalesce into the same statement. Rare paths ({@code release}, {@code markDisabled}) and
 * everything else pass straight through to the delegate.
 */
public final class GroupCommitEventStore implements EventStore {

    private final EventStore delegate;
    private final WorkerId workerId;
    private final int maxBatchSize;

    private final Queue<Entry<ProcessedMark>> processedQueue = new ConcurrentLinkedQueue<>();
    private final Queue<Entry<RetryMark>> retryQueue = new ConcurrentLinkedQueue<>();
    private final ReentrantLock flushLock = new ReentrantLock();

    /**
     * Hand-off generation: completed by an owner that releases the lock with work still queued, so
     * a waiter can take over. Replaced before it is completed, so late readers wait on a fresh one.
     * Volatile: read by waiters without the lock.
     */
    private volatile CompletableFuture<Void> flusherWanted = new CompletableFuture<>();

    /**
     * Creates the decorator.
     *
     * @param delegate the real store; its batch methods are expected to keep per-row
     *     optimistic-locking guards (ADR-0014, batch form)
     * @param workerId the engine's worker id — all batched finalizes belong to this worker, which
     *     is what makes a single shared statement possible
     * @param maxBatchSize cap on rows per flushed statement; a drain pass loops until the queues
     *     are empty, so the cap bounds statement size, not throughput
     */
    public GroupCommitEventStore(EventStore delegate, WorkerId workerId, int maxBatchSize) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        this.workerId = Objects.requireNonNull(workerId, "workerId must not be null");
        if (maxBatchSize <= 0) {
            throw new IllegalArgumentException("maxBatchSize must be > 0, got " + maxBatchSize);
        }
        this.maxBatchSize = maxBatchSize;
    }

    // ---------------------------------------------------------------------------------------------
    // batched finalize operations
    // ---------------------------------------------------------------------------------------------

    @Override
    public boolean markProcessed(UUID id, WorkerId workerId, long claimedVersion) {
        checkWorker(workerId);
        Entry<ProcessedMark> entry = new Entry<>(new ProcessedMark(id, claimedVersion));
        processedQueue.add(entry);
        return awaitFlushed(entry);
    }

    @Override
    public boolean markForRetry(
            UUID id, WorkerId workerId, long claimedVersion, String reason, Instant runAt) {
        checkWorker(workerId);
        Entry<RetryMark> entry = new Entry<>(new RetryMark(id, claimedVersion, reason, runAt));
        retryQueue.add(entry);
        return awaitFlushed(entry);
    }

    private boolean awaitFlushed(Entry<?> entry) {
        while (!entry.result.isDone()) {
            if (flushLock.tryLock()) {
                try {
                    drainAndFlushOnce();
                } finally {
                    flushLock.unlock();
                }
                // One flush per acquisition. Anything enqueued after the drain is handed to the
                // waiters — after the unlock, so the winner of the hand-off can take the lock.
                if (!queuesEmpty()) {
                    signalFlusherWanted();
                }
                continue;
            }
            // Snapshot the generation first: an owner that signals after this point completes
            // exactly this future. Then re-check the lock — an owner that released between our
            // tryLock and here may have signalled an older generation, so retry instead of waiting.
            CompletableFuture<Void> wanted = flusherWanted;
            if (entry.result.isDone() || !flushLock.isLocked()) {
                continue;
            }
            // handle(): a failed verdict must surface through the unwrap below, not from here.
            CompletableFuture.anyOf(entry.result, wanted).handle((v, ex) -> null).join();
        }
        // Done by this point; join() only unwraps the verdict or the storage failure.
        try {
            return entry.result.join();
        } catch (CompletionException ex) {
            // Surface the delegate's own exception, exactly as a direct call would have thrown it.
            if (ex.getCause() instanceof RuntimeException cause) {
                throw cause;
            }
            throw ex;
        }
    }

    private boolean queuesEmpty() {
        return processedQueue.isEmpty() && retryQueue.isEmpty();
    }

    /**
     * Wakes every waiter of the current generation so one of them becomes the next flusher. The
     * generation is replaced first: a waiter that reads {@code flusherWanted} after this point must
     * wait on the next hand-off, not on one that already happened.
     */
    private void signalFlusherWanted() {
        CompletableFuture<Void> previous = flusherWanted;
        flusherWanted = new CompletableFuture<>();
        previous.complete(null);
    }

    /**
     * Drains up to {@code maxBatchSize} entries from each queue and flushes them as one batch
     * statement per operation kind. Never throws: a delegate failure completes the drained entries'
     * futures exceptionally, and each waiting caller (including the flushing thread itself)
     * rethrows from its own future.
     */
    private void drainAndFlushOnce() {
        List<Entry<ProcessedMark>> processed = drain(processedQueue);
        if (!processed.isEmpty()) {
            flush(
                    processed,
                    marks -> delegate.markProcessedAll(marks, workerId),
                    ProcessedMark::id);
        }
        List<Entry<RetryMark>> retries = drain(retryQueue);
        if (!retries.isEmpty()) {
            flush(retries, marks -> delegate.markForRetryAll(marks, workerId), RetryMark::id);
        }
    }

    private <M> List<Entry<M>> drain(Queue<Entry<M>> queue) {
        List<Entry<M>> drained = new ArrayList<>();
        Entry<M> entry;
        while (drained.size() < maxBatchSize && (entry = queue.poll()) != null) {
            drained.add(entry);
        }
        return drained;
    }

    private <M> void flush(List<Entry<M>> entries, BatchCall<M> call, Function<M, UUID> idOf) {
        List<M> marks = new ArrayList<>(entries.size());
        for (Entry<M> e : entries) {
            marks.add(e.mark);
        }
        Set<UUID> applied;
        try {
            applied = call.apply(marks);
        } catch (RuntimeException ex) {
            for (Entry<M> e : entries) {
                e.result.completeExceptionally(ex);
            }
            return;
        }
        for (Entry<M> e : entries) {
            e.result.complete(applied.contains(idOf.apply(e.mark)));
        }
    }

    private void checkWorker(WorkerId callerWorkerId) {
        if (!this.workerId.equals(callerWorkerId)) {
            throw new IllegalArgumentException(
                    "group-commit store is bound to worker "
                            + this.workerId
                            + " but was called for "
                            + callerWorkerId);
        }
    }

    private interface BatchCall<M> {
        Set<UUID> apply(List<M> marks);
    }

    private static final class Entry<M> {
        final M mark;
        final CompletableFuture<Boolean> result = new CompletableFuture<>();

        Entry(M mark) {
            this.mark = mark;
        }
    }

    // ---------------------------------------------------------------------------------------------
    // pass-through
    // ---------------------------------------------------------------------------------------------

    @Override
    public boolean save(PendingEvent event) {
        return delegate.save(event);
    }

    @Override
    public void saveAll(List<PendingEvent> events) {
        delegate.saveAll(events);
    }

    @Override
    public Optional<UUID> lockPendingByDedupKey(String eventType, String dedupKey) {
        return delegate.lockPendingByDedupKey(eventType, dedupKey);
    }

    @Override
    public List<ClaimedEvent> claim(ClaimRequest request) {
        return delegate.claim(request);
    }

    @Override
    public Set<UUID> markProcessedAll(List<ProcessedMark> marks, WorkerId workerId) {
        return delegate.markProcessedAll(marks, workerId);
    }

    @Override
    public Set<UUID> markForRetryAll(List<RetryMark> marks, WorkerId workerId) {
        return delegate.markForRetryAll(marks, workerId);
    }

    @Override
    public boolean markDisabled(UUID id, WorkerId workerId, long claimedVersion, String reason) {
        return delegate.markDisabled(id, workerId, claimedVersion, reason);
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
    public boolean forceReclaim(UUID id, WorkerId workerId, long claimedVersion, Instant runAt) {
        return delegate.forceReclaim(id, workerId, claimedVersion, runAt);
    }

    @Override
    public int sweepStale(Duration olderThan, int limit) {
        return delegate.sweepStale(olderThan, limit);
    }

    @Override
    public int reclaimOrphans(List<WorkerId> deadWorkers, Instant now) {
        return delegate.reclaimOrphans(deadWorkers, now);
    }

    @Override
    public Optional<Event> findById(UUID id) {
        return delegate.findById(id);
    }

    @Override
    public OutboxMetricsSnapshot metricsSnapshot() {
        return delegate.metricsSnapshot();
    }
}
