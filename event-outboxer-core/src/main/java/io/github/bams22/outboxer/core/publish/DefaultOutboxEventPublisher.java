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
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EventSerializer;
import io.github.bams22.outboxer.spi.EventStore;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

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
 * </ol>
 */
public final class DefaultOutboxEventPublisher implements OutboxEventPublisher {

  private final EventStore store;
  private final EventSerializer serializer;
  private final Clock clock;
  private final TransactionContext txContext;
  private final NoTransactionPolicy noTxPolicy;
  private final OutboxListener listener;

  public DefaultOutboxEventPublisher(
      EventStore store,
      EventSerializer serializer,
      Clock clock,
      TransactionContext txContext,
      NoTransactionPolicy noTxPolicy,
      OutboxListener listener) {
    this.store = Objects.requireNonNull(store, "store must not be null");
    this.serializer = Objects.requireNonNull(serializer, "serializer must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    this.txContext = Objects.requireNonNull(txContext, "txContext must not be null");
    this.noTxPolicy = Objects.requireNonNull(noTxPolicy, "noTxPolicy must not be null");
    this.listener = Objects.requireNonNull(listener, "listener must not be null");
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

  @Override
  public UUID publish(String eventType, Object payload, @Nullable PublishOptions options) {
    validate(eventType, payload);
    PublishOptions resolved = options == null ? PublishOptions.defaults() : options;
    enforceTransactionPolicy();

    PendingEvent pending = buildPending(eventType, payload, resolved);
    try {
      store.save(pending);
    } catch (StorageException ex) {
      throw new PublishFailedException(
          "storage rejected event " + pending.id() + " of type " + eventType, ex);
    }
    emitPublished(pending);
    return pending.id();
  }

  @Override
  public List<UUID> publishAll(Collection<PublishRequest> requests) {
    Objects.requireNonNull(requests, "requests must not be null");
    if (requests.isEmpty()) {
      return List.of();
    }
    enforceTransactionPolicy();

    List<PendingEvent> pendings = new ArrayList<>(requests.size());
    List<UUID> ids = new ArrayList<>(requests.size());
    for (PublishRequest r : requests) {
      Objects.requireNonNull(r, "request element must not be null");
      validate(r.eventType(), r.payload());
      PublishOptions opts = r.options() == null ? PublishOptions.defaults() : r.options();
      PendingEvent pe = buildPending(r.eventType(), r.payload(), opts);
      pendings.add(pe);
      ids.add(pe.id());
    }

    try {
      store.saveAll(pendings);
    } catch (StorageException ex) {
      throw new PublishFailedException("publishAll(" + pendings.size() + ") failed", ex);
    }
    for (PendingEvent pe : pendings) {
      emitPublished(pe);
    }
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
        .build();
  }

  private void emitPublished(PendingEvent pe) {
    listener.onEventPublished(
        new EventPublishedInfo(pe.id(), pe.eventType(), clock.now(), pe.runAt(), pe.priority()));
  }
}
