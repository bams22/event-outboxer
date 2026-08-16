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

import io.github.bams22.outboxer.domain.WorkerId;
import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-process map of events currently being processed by the dispatcher. The watchdog task walks
 * this registry every {@code watchdogInterval} and force-reclaims any entry whose {@code startedAt}
 * is older than {@code handlerMaxRuntime} — see ADR-0014.
 *
 * <p>A force-reclaimed entry does not simply disappear: the row is back in {@code PENDING}, but the
 * thread that was running the handler is only <em>asked</em> to stop (an interrupt via {@link
 * DispatchHandle}), and code blocked on a socket without a read timeout will ignore that. Such
 * entries move to the <em>abandoned</em> set, where they stay until the dispatch finally returns —
 * which is what {@link #abandonedCount()} exposes as an honest "threads that did not yield" signal.
 *
 * <p>Both sets are keyed per <em>dispatch</em>, not per event id: a force-reclaimed row goes back
 * to {@code PENDING} and is regularly re-claimed by this same JVM while the previous dispatch is
 * still running, so two dispatches of one event id can overlap. Bookkeeping keyed by id alone would
 * let the newer dispatch erase the record of the thread the older one is still burning.
 */
public final class InFlightRegistry {

    private final ConcurrentMap<UUID, Entry> entries = new ConcurrentHashMap<>();
    private final ConcurrentMap<DispatchHandle, Abandoned> abandoned = new ConcurrentHashMap<>();

    /**
     * Register a newly-dispatched event. Idempotent — a second register with the same id replaces
     * the previous entry (which by then can only be an overlapping dispatch already moved to the
     * abandoned set).
     */
    public void register(Entry entry) {
        Objects.requireNonNull(entry, "entry must not be null");
        entries.put(entry.eventId(), entry);
    }

    /**
     * Remove all bookkeeping for one finished dispatch — in-flight or already abandoned. No-op if
     * absent. Removes only this dispatch's own records: a newer dispatch of the same event id,
     * started after this one was force-reclaimed, keeps its place in the registry.
     */
    public void unregister(Entry entry) {
        Objects.requireNonNull(entry, "entry must not be null");
        entries.remove(entry.eventId(), entry);
        abandoned.remove(entry.handle());
    }

    /**
     * Move an entry the watchdog has just force-reclaimed out of the in-flight set and into the
     * abandoned set. It is no longer a candidate for reclaim (its row belongs to whoever claims it
     * next), but its thread is still running the handler and still holds a slot of the type's
     * handler pool.
     *
     * @param entry the force-reclaimed dispatch
     * @param reclaimedAt clock time of the force-reclaim
     * @param interrupted whether the dispatching thread was interrupted
     */
    public void markAbandoned(Entry entry, Instant reclaimedAt, boolean interrupted) {
        Objects.requireNonNull(entry, "entry must not be null");
        Objects.requireNonNull(reclaimedAt, "reclaimedAt must not be null");
        if (!entries.remove(entry.eventId(), entry)) {
            // The dispatch finished between the watchdog's snapshot and this call — nothing leaked.
            return;
        }
        abandoned.put(entry.handle(), new Abandoned(entry, reclaimedAt, interrupted));
        if (!entry.handle().isActive()) {
            // Raced with the dispatch's own cleanup: it deactivated (and possibly ran its
            // unregister, finding nothing here yet) while we were moving it across. No thread is
            // held, so undo — otherwise this entry would inflate the abandoned gauge forever.
            abandoned.remove(entry.handle());
        }
    }

    /** Snapshot of every currently in-flight entry; safe for iteration. */
    public Collection<Entry> snapshot() {
        return entries.values();
    }

    /** Snapshot of every force-reclaimed dispatch whose thread has not returned yet. */
    public Collection<Abandoned> abandonedSnapshot() {
        return abandoned.values();
    }

    /** Number of in-flight events at the time of the call. */
    public int size() {
        return entries.size();
    }

    /**
     * Number of in-flight events of the given type at the time of the call. O(in-flight), which is
     * bounded by the per-type pool budgets — cheap enough for a gauge read on scrape.
     */
    public int countByType(String eventType) {
        Objects.requireNonNull(eventType, "eventType must not be null");
        int count = 0;
        for (Entry entry : entries.values()) {
            if (entry.eventType().equals(eventType)) {
                count++;
            }
        }
        return count;
    }

    /** Number of force-reclaimed dispatches whose thread is still running. */
    public int abandonedCount() {
        return abandoned.size();
    }

    /** Number of force-reclaimed dispatches of the given type whose thread is still running. */
    public int abandonedCountByType(String eventType) {
        Objects.requireNonNull(eventType, "eventType must not be null");
        int count = 0;
        for (Abandoned a : abandoned.values()) {
            if (a.entry().eventType().equals(eventType)) {
                count++;
            }
        }
        return count;
    }

    /**
     * One in-flight record.
     *
     * @param eventId id of the event being processed
     * @param eventType its type
     * @param workerId worker JVM running the handler
     * @param claimedVersion version observed at claim time — echoed to {@code forceReclaim} if the
     *     watchdog fires
     * @param startedAt clock time the dispatcher invoked {@code handler.handle(...)}; used by the
     *     watchdog's staleness check
     * @param handle cancellation handle bound to the dispatching thread
     */
    public record Entry(
            UUID eventId,
            String eventType,
            WorkerId workerId,
            long claimedVersion,
            Instant startedAt,
            DispatchHandle handle) {

        public Entry {
            Objects.requireNonNull(eventId, "eventId must not be null");
            Objects.requireNonNull(eventType, "eventType must not be null");
            Objects.requireNonNull(workerId, "workerId must not be null");
            Objects.requireNonNull(startedAt, "startedAt must not be null");
            Objects.requireNonNull(handle, "handle must not be null");
        }
    }

    /**
     * A dispatch whose row was force-reclaimed while its thread kept running. The {@code reported}
     * flag is flipped by the watchdog the first time the grace period elapses, so a thread that
     * never returns produces exactly one alert instead of one per tick.
     */
    public static final class Abandoned {

        private final Entry entry;
        private final Instant reclaimedAt;
        private final boolean interrupted;
        private volatile boolean reported;

        Abandoned(Entry entry, Instant reclaimedAt, boolean interrupted) {
            this.entry = Objects.requireNonNull(entry, "entry must not be null");
            this.reclaimedAt = Objects.requireNonNull(reclaimedAt, "reclaimedAt must not be null");
            this.interrupted = interrupted;
        }

        public Entry entry() {
            return entry;
        }

        public Instant reclaimedAt() {
            return reclaimedAt;
        }

        /** Whether the dispatching thread was interrupted when the row was force-reclaimed. */
        public boolean interrupted() {
            return interrupted;
        }

        /**
         * Claim the one-shot right to report this dispatch as leaked.
         *
         * @return {@code true} for the first caller only
         */
        public synchronized boolean claimReport() {
            if (reported) {
                return false;
            }
            reported = true;
            return true;
        }
    }
}
