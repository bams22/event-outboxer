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

import java.time.Duration;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Result of processing an event. Sealed — exactly four outcomes are possible: {@link Success},
 * {@link Retry}, {@link Fail}, and {@link Skip}.
 *
 * <p>An uncaught exception thrown from {@code EventHandler.handle(...)} is treated by the engine
 * as {@code Retry(ex.getMessage(), null, ex)}. Handlers therefore do not need to convert every
 * exception to a {@code Retry} manually — returning {@code Success}, {@code Skip}, or an explicit
 * {@code Fail} is enough.
 */
public sealed interface EventOutcome permits
    EventOutcome.Success, EventOutcome.Retry, EventOutcome.Fail, EventOutcome.Skip {

  /**
   * Processing completed successfully. The event is removed from the active outbox (or moved to
   * the archive if archiving is enabled).
   */
  record Success() implements EventOutcome {

    /**
     * Canonical zero-state instance. Prefer {@code EventOutcome.Success.INSTANCE} in handler
     * code instead of allocating a new record.
     */
    public static final Success INSTANCE = new Success();
  }

  /**
   * Transient failure — re-schedule the event for another attempt.
   *
   * @param reason human-readable reason written to {@code last_fail_reason}
   * @param delayOverride optional explicit delay before the next attempt; if null, the
   *     configured {@code FailureHandler} chain decides (typically exponential backoff)
   * @param cause originating exception, or null if the handler returned {@code Retry} without
   *     one
   */
  record Retry(String reason, @Nullable Duration delayOverride, @Nullable Throwable cause)
      implements EventOutcome {

    public Retry {
      Objects.requireNonNull(reason, "reason must not be null");
    }
  }

  /**
   * Non-retryable failure — move the event straight to {@code DISABLED}. Use when the handler
   * knows that additional attempts cannot succeed (for example, a permanent {@code 404} from an
   * external API).
   *
   * @param reason human-readable reason written to {@code last_fail_reason}
   * @param cause originating exception, or null
   */
  record Fail(String reason, @Nullable Throwable cause) implements EventOutcome {

    public Fail {
      Objects.requireNonNull(reason, "reason must not be null");
    }
  }

  /**
   * Successful no-op — the handler noticed the event has already been processed through another
   * path (typical idempotency check) and does not need to do any work. Treated as {@code
   * Success} by storage but distinguished in observability (separate listener callback, separate
   * metric).
   *
   * @param reason human-readable reason written to logs; may be useful when investigating
   *     metrics later
   */
  record Skip(String reason) implements EventOutcome {

    public Skip {
      Objects.requireNonNull(reason, "reason must not be null");
    }
  }
}
