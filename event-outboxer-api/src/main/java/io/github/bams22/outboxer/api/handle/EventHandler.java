/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.api.handle;

import org.jspecify.annotations.Nullable;

/**
 * Business handler for a specific event type. Implementations are typically registered as Spring
 * beans and discovered automatically at application startup; in plain-Java setups they are
 * passed to the engine builder.
 *
 * <h2>Idempotency contract</h2>
 *
 * event-outboxer guarantees <strong>at-least-once</strong> delivery (see ADR-0015). An event may
 * be processed more than once if:
 *
 * <ul>
 *   <li>a worker completed the handler but crashed before finalizing;
 *   <li>orphan recovery reclaimed an event whose worker was merely slow (false positive);
 *   <li>the watchdog force-reclaimed an event whose handler was running longer than {@code
 *       handlerMaxRuntime};
 *   <li>the handler threw a transient exception after already applying its side effect.
 * </ul>
 *
 * Handlers MUST be idempotent. Common patterns: natural idempotency (PUT-style updates),
 * idempotency keys on remote API calls, check-then-act against local state, or an explicit
 * dedup-table keyed by {@code EventContext.eventId()}.
 *
 * <h2>Exception handling</h2>
 *
 * Any uncaught exception thrown by {@link #handle(EventContext, Object)} is treated by the
 * engine as {@code EventOutcome.Retry(e.getMessage(), null, e)} and routed through the configured
 * {@code FailureHandler} chain. Handlers therefore do not need to wrap everything in try/catch —
 * return {@code Success}/{@code Skip} on the happy path, and let the engine handle the rest.
 *
 * @param <T> payload type for this event type
 */
public interface EventHandler<T> {

  /**
   * Stable string identifier of the event type. Used to bind the handler to events stored with
   * the same {@code event_type} value. Must not change between releases — it is a natural key in
   * the database.
   */
  String eventType();

  /**
   * Java class of the payload. The engine uses this to drive strict deserialization of the JSON
   * stored alongside the event.
   */
  Class<T> payloadType();

  /**
   * Process the event. See {@linkplain EventHandler class-level Javadoc} for the idempotency
   * contract and exception handling semantics.
   */
  EventOutcome handle(EventContext ctx, T payload);

  /**
   * Optional business-key lock guarding concurrent processing of events that touch the same
   * aggregate. If non-null, the engine acquires the lock through {@code EntityLocker.tryLock}
   * before {@link #handle}; if the lock is busy, the event is re-scheduled with a short delay
   * and {@code handle} is not invoked (see ADR-0012).
   *
   * <p>Default: {@code null} — no locking. Override when two events of potentially different
   * types can target the same aggregate and must not run in parallel.
   */
  default @Nullable String extractLockKey(T payload) {
    return null;
  }

  /**
   * Optional per-handler override of the failure strategy. When non-null, it wins over the
   * starter-configured per-type failure handler and the global default. Use it for handlers that
   * need a different retry policy than everything else (for example, a validation handler that
   * should go straight to {@code Fail}).
   *
   * <p>Default: {@code null} — fall back to the per-type or global chain.
   */
  default @Nullable FailureHandler<T> failureHandler() {
    return null;
  }
}
