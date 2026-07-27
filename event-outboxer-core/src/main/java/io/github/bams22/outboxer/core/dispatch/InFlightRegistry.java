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
 */
public final class InFlightRegistry {

  private final ConcurrentMap<UUID, Entry> entries = new ConcurrentHashMap<>();

  /**
   * Register a newly-dispatched event. Idempotent — a second register with the same id replaces the
   * previous entry.
   */
  public void register(UUID id, Entry entry) {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(entry, "entry must not be null");
    entries.put(id, entry);
  }

  /** Remove the entry for a finalized event. No-op if absent. */
  public void unregister(UUID id) {
    Objects.requireNonNull(id, "id must not be null");
    entries.remove(id);
  }

  /** Snapshot of every currently in-flight entry; safe for iteration. */
  public Collection<Entry> snapshot() {
    return entries.values();
  }

  /** Number of in-flight events at the time of the call. */
  public int size() {
    return entries.size();
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
   */
  public record Entry(
      UUID eventId, String eventType, WorkerId workerId, long claimedVersion, Instant startedAt) {

    public Entry {
      Objects.requireNonNull(eventId, "eventId must not be null");
      Objects.requireNonNull(eventType, "eventType must not be null");
      Objects.requireNonNull(workerId, "workerId must not be null");
      Objects.requireNonNull(startedAt, "startedAt must not be null");
    }
  }
}
