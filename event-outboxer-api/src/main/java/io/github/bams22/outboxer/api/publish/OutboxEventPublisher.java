/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.api.publish;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Public entry point for persisting events into the outbox.
 *
 * <h2>Transactional contract</h2>
 *
 * {@code publish(...)} and {@code publishAll(...)} MUST participate in the caller's current
 * transaction: if the transaction commits, the event becomes visible to the engine; if it rolls
 * back, the event is not persisted. The Spring Boot starter implements this by wrapping the
 * {@code DataSource} in a {@code TransactionAwareDataSourceProxy} (see ADR-0002).
 *
 * <p>When the publisher is invoked outside a transaction, behavior depends on the configured
 * policy {@code outbox.publisher.no-transaction-policy}:
 *
 * <ul>
 *   <li>{@code FAIL} (default) — throws {@code NoTransactionException}.
 *   <li>{@code AUTO} — opens a short-lived autonomous transaction (loses atomicity with the
 *       caller's state; intended for simple scripts).
 *   <li>{@code IGNORE} — writes without a transaction (unsafe; for tests only).
 * </ul>
 *
 * <h2>Error semantics</h2>
 *
 * All publish failures raise a subclass of {@code PublishException}:
 *
 * <ul>
 *   <li>{@code PublishValidationException} — bad arguments.
 *   <li>{@code PublishSerializationException} — the payload could not be serialized.
 *   <li>{@code NoTransactionException} — no active transaction under the FAIL policy.
 *   <li>{@code PublishFailedException} — {@code publishAll(...)} encountered a storage-level
 *       failure.
 * </ul>
 *
 * <p>{@code publishAll(...)} is fail-fast and all-or-nothing: on the first failure it throws
 * and the caller's transaction is expected to roll back, leaving the outbox untouched.
 *
 * <h2>Thread-safety</h2>
 *
 * Implementations MUST be thread-safe.
 */
public interface OutboxEventPublisher {

  /** Equivalent to {@code publish(eventType, payload, PublishOptions.defaults())}. */
  UUID publish(String eventType, Object payload);

  /** Equivalent to {@code publish(eventType, payload, PublishOptions.builder().runAt(runAt).build())}. */
  UUID publish(String eventType, Object payload, Instant runAt);

  /**
   * Publishes a single event with explicit options.
   *
   * @return the id of the persisted event; the same value is visible through {@code Event.id()}
   */
  UUID publish(String eventType, Object payload, PublishOptions options);

  /**
   * Publishes a batch of events fail-fast. On success the returned list has the same size and
   * order as {@code requests}; on any failure a {@code PublishFailedException} is raised and the
   * caller's transaction is expected to roll back, leaving the outbox untouched.
   */
  List<UUID> publishAll(Collection<PublishRequest> requests);

  /**
   * Publishes the event unless another event with the same {@code idempotencyKey} has already
   * been persisted. Returns {@code Optional.empty()} on collision — this is a deduplication
   * outcome, not an error (see ADR Q3).
   */
  Optional<UUID> publishIfAbsent(
      String eventType, Object payload, String idempotencyKey, PublishOptions options);
}
