/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.support;

import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.OutboxMetricsSnapshot;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Test double base: forwards every {@link EventStore} call to a delegate. Subclasses override
 * only what the test needs. Exists because {@code InMemoryEventStore} is final and cannot be
 * subclassed directly.
 */
public class ForwardingEventStore implements EventStore {

  protected final EventStore delegate;

  public ForwardingEventStore(EventStore delegate) {
    this.delegate = delegate;
  }

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
  public boolean markProcessed(UUID id, WorkerId workerId, long claimedVersion) {
    return delegate.markProcessed(id, workerId, claimedVersion);
  }

  @Override
  public boolean markForRetry(
      UUID id, WorkerId workerId, long claimedVersion, String reason, Instant runAt) {
    return delegate.markForRetry(id, workerId, claimedVersion, reason, runAt);
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
