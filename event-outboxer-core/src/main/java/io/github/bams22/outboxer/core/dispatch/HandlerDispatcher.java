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

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.api.handle.FailureContext;
import io.github.bams22.outboxer.api.handle.FailureDecision;
import io.github.bams22.outboxer.api.handle.FailureHandler;
import io.github.bams22.outboxer.api.observer.EventClaimedInfo;
import io.github.bams22.outboxer.api.observer.EventDeletedInfo;
import io.github.bams22.outboxer.api.observer.EventDisabledInfo;
import io.github.bams22.outboxer.api.observer.EventProcessedInfo;
import io.github.bams22.outboxer.api.observer.EventRetryScheduledInfo;
import io.github.bams22.outboxer.api.observer.EventSkippedInfo;
import io.github.bams22.outboxer.api.observer.HandlerErrorInfo;
import io.github.bams22.outboxer.api.observer.LockAcquisitionInfo;
import io.github.bams22.outboxer.api.observer.LockReleaseInfo;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.observer.SerializationErrorInfo;
import io.github.bams22.outboxer.api.observer.UnknownEventTypeInfo;
import io.github.bams22.outboxer.core.config.EventTypeConfig;
import io.github.bams22.outboxer.core.config.EventTypeConfigProvider;
import io.github.bams22.outboxer.core.config.UnknownHandlerPolicy;
import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.spi.EntityLocker.LockHandle;
import io.github.bams22.outboxer.spi.EventSerializer;
import io.github.bams22.outboxer.spi.EventStore;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes a single {@link ClaimedEvent}: looks up the handler, deserialises the payload,
 * acquires an optional business-key lock, invokes {@code handler.handle(...)}, and routes the
 * outcome through the storage finalize methods and the {@link OutboxListener}.
 *
 * <p>One dispatcher instance is shared across per-type handler executors; it holds no mutable
 * state and is safe for concurrent use.
 */
public final class HandlerDispatcher {

  private static final Logger log = LoggerFactory.getLogger(HandlerDispatcher.class);

  private final EventStore store;
  private final EntityLocker locker;
  private final EventSerializer serializer;
  private final EventHandlerResolver handlers;
  private final FailureHandlerResolver failureHandlers;
  private final InFlightRegistry inFlight;
  private final OutboxListener listener;
  private final Clock clock;
  private final WorkerId workerId;
  private final EventTypeConfigProvider typeConfig;
  private final DispatcherConfig dispatcherConfig;

  public HandlerDispatcher(
      EventStore store,
      EntityLocker locker,
      EventSerializer serializer,
      EventHandlerResolver handlers,
      FailureHandlerResolver failureHandlers,
      InFlightRegistry inFlight,
      OutboxListener listener,
      Clock clock,
      WorkerId workerId,
      EventTypeConfigProvider typeConfig,
      DispatcherConfig dispatcherConfig) {
    this.store = Objects.requireNonNull(store);
    this.locker = Objects.requireNonNull(locker);
    this.serializer = Objects.requireNonNull(serializer);
    this.handlers = Objects.requireNonNull(handlers);
    this.failureHandlers = Objects.requireNonNull(failureHandlers);
    this.inFlight = Objects.requireNonNull(inFlight);
    this.listener = Objects.requireNonNull(listener);
    this.clock = Objects.requireNonNull(clock);
    this.workerId = Objects.requireNonNull(workerId);
    this.typeConfig = Objects.requireNonNull(typeConfig);
    this.dispatcherConfig = Objects.requireNonNull(dispatcherConfig);
  }

  /** Entry point invoked by the per-type handler executor for each claimed event. */
  public void dispatch(ClaimedEvent claimed) {
    Objects.requireNonNull(claimed, "claimed must not be null");
    listener.onEventClaimed(
        new EventClaimedInfo(
            claimed.id(), claimed.eventType(), claimed.attempts() + 1, claimed.claimedAt(), workerId));

    Optional<EventHandler<?>> handlerOpt = handlers.find(claimed.eventType());
    if (handlerOpt.isEmpty()) {
      handleUnknownType(claimed);
      return;
    }
    EventHandler<?> handler = handlerOpt.get();

    Object payload;
    try {
      payload = deserialise(handler, claimed);
    } catch (RuntimeException ex) {
      listener.onEventSerializationError(
          new SerializationErrorInfo(claimed.id(), claimed.eventType(), claimed.payloadClass(), ex));
      String reason = "payload deserialization failed: " + rootMessage(ex);
      finaliseDisable(claimed, reason, ex);
      return;
    }

    EventTypeConfig cfg = typeConfig.forType(claimed.eventType());
    @Nullable String lockKey = extractLockKey(handler, payload);
    @Nullable LockHandle lock = null;
    try {
      if (lockKey != null) {
        lock = tryAcquireLock(claimed, lockKey, cfg.lockTtl());
        if (lock == null) {
          // busy or acquisition error — already rescheduled inside tryAcquireLock
          return;
        }
      }
      EventOutcome outcome = invokeHandler(claimed, handler, payload);
      routeOutcome(claimed, handler, payload, outcome);
    } finally {
      closeLock(claimed, lockKey, lock);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // helpers: unknown type
  // ---------------------------------------------------------------------------------------------

  private void handleUnknownType(ClaimedEvent claimed) {
    listener.onUnknownEventType(new UnknownEventTypeInfo(claimed.id(), claimed.eventType()));
    switch (dispatcherConfig.unknownHandlerPolicy()) {
      case SKIP -> {
        Instant when = clock.now().plus(dispatcherConfig.unknownHandlerRetryDelay());
        boolean ok =
            store.markForRetry(
                claimed.id(),
                workerId,
                claimed.claimedVersion(),
                "no handler for eventType " + claimed.eventType(),
                when);
        if (ok) {
          listener.onEventRetryScheduled(
              new EventRetryScheduledInfo(
                  claimed.id(),
                  claimed.eventType(),
                  claimed.attempts() + 1,
                  when,
                  "unknown handler",
                  null));
        }
      }
      case DISABLE ->
          finaliseDisable(claimed, "no handler for eventType " + claimed.eventType(), null);
      case FAIL -> {
        // Leave the row PROCESSING; orphan recovery / watchdog will pick it up.
        log.error(
            "unknown handler for eventType={} (policy=FAIL); leaving event PROCESSING",
            claimed.eventType());
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // helpers: deserialisation + lock + invocation
  // ---------------------------------------------------------------------------------------------

  private Object deserialise(EventHandler<?> handler, ClaimedEvent claimed) {
    Class<?> payloadType = handler.payloadType();
    return serializer.deserialize(claimed.payload(), payloadType);
  }

  private @Nullable String extractLockKey(EventHandler<?> handler, Object payload) {
    return extractLockKeyTyped(handler, payload);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private @Nullable String extractLockKeyTyped(EventHandler handler, Object payload) {
    return handler.extractLockKey(payload);
  }

  private @Nullable LockHandle tryAcquireLock(ClaimedEvent claimed, String lockKey, Duration ttl) {
    Optional<LockHandle> held;
    try {
      held = locker.tryLock(lockKey, ttl);
    } catch (RuntimeException ex) {
      listener.onLockAcquisitionFailed(
          new LockAcquisitionInfo(claimed.id(), claimed.eventType(), lockKey));
      Instant when = clock.now().plus(dispatcherConfig.lockBusyRetryDelay());
      store.markForRetry(
          claimed.id(),
          workerId,
          claimed.claimedVersion(),
          "lock acquisition error: " + rootMessage(ex),
          when);
      return null;
    }
    if (held.isEmpty()) {
      listener.onLockAcquisitionFailed(
          new LockAcquisitionInfo(claimed.id(), claimed.eventType(), lockKey));
      Instant when = clock.now().plus(dispatcherConfig.lockBusyRetryDelay());
      boolean ok =
          store.markForRetry(
              claimed.id(),
              workerId,
              claimed.claimedVersion(),
              "lock busy: " + lockKey,
              when);
      if (ok) {
        listener.onEventRetryScheduled(
            new EventRetryScheduledInfo(
                claimed.id(),
                claimed.eventType(),
                claimed.attempts() + 1,
                when,
                "lock busy",
                null));
      }
      return null;
    }
    return held.get();
  }

  private void closeLock(ClaimedEvent claimed, @Nullable String lockKey, @Nullable LockHandle lock) {
    if (lock == null) {
      return;
    }
    try {
      lock.close();
    } catch (RuntimeException ex) {
      listener.onLockReleaseFailed(
          new LockReleaseInfo(
              claimed.id(), claimed.eventType(), lockKey != null ? lockKey : "", ex));
    }
  }

  private EventOutcome invokeHandler(ClaimedEvent claimed, EventHandler<?> handler, Object payload) {
    Instant started = clock.now();
    inFlight.register(
        claimed.id(),
        new InFlightRegistry.Entry(
            claimed.id(), claimed.eventType(), workerId, claimed.claimedVersion(), started));
    try {
      EventContext ctx =
          new EventContext(
              claimed.id(),
              claimed.eventType(),
              claimed.attempts() + 1,
              claimed.createdAt(),
              claimed.claimedAt(),
              workerId,
              claimed.traceContext());
      return invokeHandlerTyped(handler, ctx, payload);
    } catch (RuntimeException ex) {
      listener.onHandlerError(
          new HandlerErrorInfo(claimed.id(), claimed.eventType(), claimed.attempts() + 1, ex));
      String msg = ex.getMessage();
      return new EventOutcome.Retry(msg != null ? msg : ex.getClass().getSimpleName(), null, ex);
    } finally {
      inFlight.unregister(claimed.id());
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private EventOutcome invokeHandlerTyped(EventHandler handler, EventContext ctx, Object payload) {
    return handler.handle(ctx, payload);
  }

  // ---------------------------------------------------------------------------------------------
  // helpers: outcome routing
  // ---------------------------------------------------------------------------------------------

  private void routeOutcome(
      ClaimedEvent claimed, EventHandler<?> handler, Object payload, EventOutcome outcome) {
    // instanceof chain instead of pattern-matching switch — baseline is Java 17 (ADR-0017).
    // The final throw preserves exhaustiveness at runtime: if a new EventOutcome subtype is
    // added without updating this dispatcher, the unhandled case surfaces immediately.
    if (outcome instanceof EventOutcome.Success) {
      finaliseSuccess(claimed);
    } else if (outcome instanceof EventOutcome.Skip skip) {
      finaliseSkip(claimed, skip);
    } else if (outcome instanceof EventOutcome.Retry retry) {
      handleRetryOutcome(claimed, handler, payload, retry);
    } else if (outcome instanceof EventOutcome.Fail fail) {
      handleFailOutcome(claimed, handler, payload, fail);
    } else {
      throw new IllegalStateException("unhandled EventOutcome: " + outcome.getClass());
    }
  }

  private void finaliseSuccess(ClaimedEvent claimed) {
    boolean ok = store.markProcessed(claimed.id(), workerId, claimed.claimedVersion());
    if (ok) {
      Duration duration = Duration.between(claimed.claimedAt(), clock.now());
      listener.onEventProcessed(
          new EventProcessedInfo(
              claimed.id(), claimed.eventType(), claimed.attempts() + 1, duration));
    } else {
      log.debug(
          "markProcessed lost race for eventId={} (version bumped by watchdog / orphan recovery)",
          claimed.id());
    }
  }

  private void finaliseSkip(ClaimedEvent claimed, EventOutcome.Skip skip) {
    boolean ok = store.markProcessed(claimed.id(), workerId, claimed.claimedVersion());
    if (ok) {
      listener.onEventSkipped(
          new EventSkippedInfo(claimed.id(), claimed.eventType(), skip.reason()));
    }
  }

  private void finaliseDisable(ClaimedEvent claimed, String reason, @Nullable Throwable cause) {
    boolean ok = store.markDisabled(claimed.id(), workerId, claimed.claimedVersion(), reason);
    if (ok) {
      listener.onEventDisabled(
          new EventDisabledInfo(
              claimed.id(), claimed.eventType(), claimed.attempts() + 1, reason, cause));
    }
  }

  private void handleRetryOutcome(
      ClaimedEvent claimed, EventHandler<?> handler, Object payload, EventOutcome.Retry retry) {
    if (retry.delayOverride() != null) {
      Instant when = clock.now().plus(retry.delayOverride());
      applyRetry(claimed, when, retry.reason(), retry.cause());
      return;
    }
    applyFailureDecision(claimed, handler, payload, retry, retry.cause());
  }

  private void handleFailOutcome(
      ClaimedEvent claimed, EventHandler<?> handler, Object payload, EventOutcome.Fail fail) {
    applyFailureDecision(claimed, handler, payload, fail, fail.cause());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void applyFailureDecision(
      ClaimedEvent claimed,
      EventHandler<?> handler,
      @Nullable Object payload,
      EventOutcome outcome,
      @Nullable Throwable cause) {
    FailureHandler fh = failureHandlers.resolve(handler);
    FailureContext ctx =
        new FailureContext(claimed, payload, outcome, cause, claimed.attempts() + 1, clock.now());
    FailureDecision decision;
    try {
      decision = fh.onFailure(ctx);
    } catch (RuntimeException ex) {
      log.warn("failure handler threw; defaulting to Disable", ex);
      decision = new FailureDecision.Disable("failure handler threw: " + ex);
    }
    if (decision instanceof FailureDecision.RetryAt ra) {
      applyRetry(claimed, ra.when(), ra.reason(), cause);
    } else if (decision instanceof FailureDecision.Disable d) {
      finaliseDisable(claimed, d.reason(), cause);
    } else if (decision instanceof FailureDecision.Delete del) {
      applyDelete(claimed, del.reason());
    } else {
      throw new IllegalStateException("unhandled FailureDecision: " + decision.getClass());
    }
  }

  private void applyRetry(
      ClaimedEvent claimed, Instant when, String reason, @Nullable Throwable cause) {
    boolean ok =
        store.markForRetry(claimed.id(), workerId, claimed.claimedVersion(), reason, when);
    if (ok) {
      listener.onEventRetryScheduled(
          new EventRetryScheduledInfo(
              claimed.id(),
              claimed.eventType(),
              claimed.attempts() + 1,
              when,
              reason,
              cause));
    }
  }

  private void applyDelete(ClaimedEvent claimed, String reason) {
    boolean ok = store.markProcessed(claimed.id(), workerId, claimed.claimedVersion());
    if (ok) {
      listener.onEventDeleted(
          new EventDeletedInfo(claimed.id(), claimed.eventType(), claimed.attempts() + 1, reason));
    }
  }

  private static String rootMessage(Throwable ex) {
    String msg = ex.getMessage();
    return msg != null ? msg : ex.getClass().getSimpleName();
  }
}
