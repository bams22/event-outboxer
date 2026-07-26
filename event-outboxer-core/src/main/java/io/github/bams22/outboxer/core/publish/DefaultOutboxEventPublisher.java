/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.publish;

import io.github.bams22.outboxer.api.observer.EventPublishedInfo;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.publish.OutboxEventPublisher;
import io.github.bams22.outboxer.api.publish.PublishOptions;
import io.github.bams22.outboxer.api.publish.PublishRequest;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.exception.NoTransactionException;
import io.github.bams22.outboxer.domain.exception.PublishFailedException;
import io.github.bams22.outboxer.domain.exception.PublishSerializationException;
import io.github.bams22.outboxer.domain.exception.PublishValidationException;
import io.github.bams22.outboxer.domain.exception.StorageException;
import io.github.bams22.outboxer.core.polling.PollerWaker;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EventSerializer;
import io.github.bams22.outboxer.spi.EventStore;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default {@link OutboxEventPublisher} implementation. Holds no per-call state — a single
 * instance serves every thread. Steps:
 *
 * <ol>
 *   <li>Consult the {@link TransactionContext}; if no active transaction, apply the configured
 *       {@link NoTransactionPolicy}.
 *   <li>Serialize the payload via {@link EventSerializer}.
 *   <li>Build a {@link PendingEvent} and hand it to {@link EventStore#save(PendingEvent)}.
 *   <li>Fire {@code OutboxListener.onEventPublished(...)}.
 *   <li>Register an after-commit hook that wakes the local poller of the published type, so
 *       same-JVM publish→handle latency is bounded by the handler, not by the poll interval.
 * </ol>
 */
public final class DefaultOutboxEventPublisher implements OutboxEventPublisher {

  private static final Logger log = LoggerFactory.getLogger(DefaultOutboxEventPublisher.class);

  private final EventStore store;
  private final EventSerializer serializer;
  private final Clock clock;
  private final TransactionContext txContext;
  private final NoTransactionPolicy noTxPolicy;
  private final OutboxListener listener;
  private final PollerWaker waker;

  public DefaultOutboxEventPublisher(
      EventStore store,
      EventSerializer serializer,
      Clock clock,
      TransactionContext txContext,
      NoTransactionPolicy noTxPolicy,
      OutboxListener listener,
      PollerWaker waker) {
    this.store = Objects.requireNonNull(store, "store must not be null");
    this.serializer = Objects.requireNonNull(serializer, "serializer must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    this.txContext = Objects.requireNonNull(txContext, "txContext must not be null");
    this.noTxPolicy = Objects.requireNonNull(noTxPolicy, "noTxPolicy must not be null");
    this.listener = Objects.requireNonNull(listener, "listener must not be null");
    this.waker = Objects.requireNonNull(waker, "waker must not be null");
  }

  @Override
  public UUID publish(String eventType, Object payload) {
    return publish(eventType, payload, PublishOptions.defaults());
  }

  @Override
  public UUID publish(String eventType, Object payload, Instant runAt) {
    Objects.requireNonNull(runAt, "runAt must not be null");
    return publish(eventType, payload, PublishOptions.builder().runAt(runAt).build());
  }

  /**
   * Bounded retry for the coalescing race: the conditional insert conflicted, but the
   * conflicting PENDING row got claimed or finalized before we could lock it — re-insert.
   */
  private static final int DEDUP_RACE_RETRIES = 3;

  @Override
  public UUID publish(String eventType, Object payload, @Nullable PublishOptions options) {
    validate(eventType, payload);
    PublishOptions resolved = options == null ? PublishOptions.defaults() : options;
    enforceTransactionPolicy();

    PendingEvent pending = buildPending(eventType, payload, resolved);
    try {
      if (pending.dedupKey() != null) {
        CoalescingResult result = saveCoalescing(pending);
        if (!result.inserted()) {
          // Coalesced into an existing PENDING event. No listener, no wake — nothing new was
          // inserted, and the existing row is pinned (FOR UPDATE) inside this transaction, so
          // claims skip it until we commit and its handler is guaranteed to see our changes.
          return result.existingId();
        }
      } else {
        store.save(pending);
      }
    } catch (StorageException ex) {
      throw new PublishFailedException(
          "storage rejected event " + pending.id() + " of type " + eventType, ex);
    }
    emitPublished(pending);
    scheduleWake(Set.of(eventType));
    return pending.id();
  }

  private record CoalescingResult(boolean inserted, @Nullable UUID existingId) {}

  /**
   * ADR-0021 coalescing insert. Each loop iteration ends in exactly one of three outcomes:
   *
   * <ol>
   *   <li><b>Inserted</b> — no PENDING event with this {@code (type, key)} existed; the partial
   *       unique index arbitrated atomically ({@code ON CONFLICT}), which is why the insert goes
   *       FIRST: the common case (first publish of a key) resolves in a single statement, and a
   *       lock-first ordering would have the mirrored insert-after-check race anyway.
   *   <li><b>Coalesced</b> — the conflicting PENDING row was found and locked ({@code SELECT ...
   *       FOR UPDATE}) inside the caller's transaction. From this moment the claim query
   *       ({@code FOR UPDATE SKIP LOCKED}) skips the row until our commit, so its handler is
   *       guaranteed to observe this transaction's changes. If the lock lands on a DIFFERENT
   *       row than the one that caused the conflict (old one finalized, another publisher
   *       inserted anew), that is equally correct: any pinned PENDING event of this key carries
   *       the work and runs after our commit.
   *   <li><b>Vanished</b> — the row that conflicted with our insert is no longer PENDING by the
   *       time we try to lock it (claimed, possibly already finalized). This is not a missed
   *       race but the semantically REQUIRED branch: an event claimed before our commit may run
   *       against a snapshot without our changes, so coalescing into it would lose our update —
   *       we must loop and insert our own event (the old row no longer occupies the
   *       PENDING-scoped unique index, so the retry succeeds unless yet another publisher got
   *       there first).
   * </ol>
   *
   * <p>The lock probe cannot read stale state: if the row is being claimed concurrently, our
   * {@code SELECT ... FOR UPDATE} blocks on the claim's row lock and PostgreSQL re-evaluates
   * the {@code status = 'PENDING'} predicate against the committed row version afterwards
   * (EvalPlanQual) — we either hold the lock on a genuinely PENDING row or see empty.
   *
   * <p>Reaching the retry bound requires a fresh PENDING row of the same key to appear AND
   * disappear in the microsecond window between our conflict and our lock, {@code
   * DEDUP_RACE_RETRIES} times in a row — a pathological churn that the bound converts from a
   * theoretical livelock into a loud failure.
   */
  private CoalescingResult saveCoalescing(PendingEvent pending) {
    for (int attempt = 0; attempt < DEDUP_RACE_RETRIES; attempt++) {
      if (store.save(pending)) {
        return new CoalescingResult(true, null);
      }
      var existing = store.lockPendingByDedupKey(pending.eventType(), pending.dedupKey());
      if (existing.isPresent()) {
        return new CoalescingResult(false, existing.get());
      }
      // The PENDING row vanished under us — loop and insert our own.
    }
    throw new PublishFailedException(
        "could not publish event with dedupKey '"
            + pending.dedupKey()
            + "' of type "
            + pending.eventType()
            + " after "
            + DEDUP_RACE_RETRIES
            + " attempts (pathological claim/finalize churn on the key)",
        null);
  }

  @Override
  public List<UUID> publishAll(Collection<PublishRequest> requests) {
    Objects.requireNonNull(requests, "requests must not be null");
    if (requests.isEmpty()) {
      return List.of();
    }
    enforceTransactionPolicy();

    // Requests with a dedup key need per-row coalescing feedback and go through save(...) one
    // by one; the rest batch through saveAll. Returned ids stay aligned with request order.
    List<PendingEvent> batch = new ArrayList<>(requests.size());
    List<UUID> ids = new ArrayList<>(requests.size());
    List<PendingEvent> inserted = new ArrayList<>(requests.size());
    try {
      for (PublishRequest r : requests) {
        Objects.requireNonNull(r, "request element must not be null");
        validate(r.eventType(), r.payload());
        PublishOptions opts = r.options() == null ? PublishOptions.defaults() : r.options();
        PendingEvent pe = buildPending(r.eventType(), r.payload(), opts);
        if (pe.dedupKey() != null) {
          CoalescingResult result = saveCoalescing(pe);
          if (result.inserted()) {
            inserted.add(pe);
            ids.add(pe.id());
          } else {
            ids.add(result.existingId());
          }
        } else {
          batch.add(pe);
          ids.add(pe.id());
        }
      }
      store.saveAll(batch);
      inserted.addAll(batch);
    } catch (StorageException ex) {
      throw new PublishFailedException("publishAll(" + requests.size() + ") failed", ex);
    }
    for (PendingEvent pe : inserted) {
      emitPublished(pe);
    }
    Set<String> types = new LinkedHashSet<>();
    for (PendingEvent pe : inserted) {
      types.add(pe.eventType());
    }
    scheduleWake(types);
    return List.copyOf(ids);
  }

  // ---------------------------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------------------------

  private void validate(String eventType, Object payload) {
    if (eventType == null) {
      throw new PublishValidationException("eventType must not be null");
    }
    if (eventType.isBlank()) {
      throw new PublishValidationException("eventType must not be blank");
    }
    if (payload == null) {
      throw new PublishValidationException("payload must not be null");
    }
  }

  private void enforceTransactionPolicy() {
    if (txContext.isActive()) {
      return;
    }
    if (noTxPolicy == NoTransactionPolicy.FAIL) {
      throw new NoTransactionException(
          "OutboxEventPublisher.publish must be invoked inside a transaction "
              + "(configure noTransactionPolicy=IGNORE to opt out).");
    }
  }

  private PendingEvent buildPending(String eventType, Object payload, PublishOptions options) {
    String serialized;
    try {
      serialized = serializer.serialize(payload);
    } catch (PublishSerializationException ex) {
      throw ex;
    } catch (RuntimeException ex) {
      throw new PublishSerializationException(
          "failed to serialize payload of type " + payload.getClass().getName(), ex);
    }
    Instant runAt = options.runAt() != null ? options.runAt() : clock.now();
    short priority = options.priority() != null ? options.priority() : (short) 0;
    Map<String, String> traceContext =
        options.traceContext() != null ? options.traceContext() : Map.of();
    return PendingEvent.builder()
        .id(UUID.randomUUID())
        .eventType(eventType)
        .payload(serialized)
        .payloadClass(payload.getClass().getName())
        .priority(priority)
        .runAt(runAt)
        .traceContext(traceContext)
        .dedupKey(options.dedupKey())
        .build();
  }

  private void emitPublished(PendingEvent pe) {
    listener.onEventPublished(
        new EventPublishedInfo(pe.id(), pe.eventType(), clock.now(), pe.runAt(), pe.priority()));
  }

  /**
   * Wake the local pollers of the published types once the surrounding transaction commits.
   * Purely an optimization — every path is swallowed on failure so a wake can never break the
   * caller's commit.
   */
  private void scheduleWake(Set<String> eventTypes) {
    try {
      txContext.afterCommit(
          () -> {
            for (String type : eventTypes) {
              try {
                waker.wake(type);
              } catch (RuntimeException ex) {
                log.debug("poller wake failed for type {}: {}", type, ex.toString());
              }
            }
          });
    } catch (RuntimeException ex) {
      log.debug("afterCommit registration failed: {}", ex.toString());
    }
  }
}
