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
 * <p>An uncaught exception thrown from {@code EventHandler.handle(...)} is treated by the engine as
 * {@code retry(ex.getMessage(), ex)}. Handlers therefore do not need to convert every exception to
 * a {@code Retry} manually — returning {@link #success()}, {@link #skip(String)}, or an explicit
 * {@link #fail(String)} is enough.
 *
 * <p>The static factories are the idiomatic way to produce an outcome; the records stay public for
 * pattern matching ({@code switch (outcome) { case Retry r -> ... }}).
 */
public sealed interface EventOutcome
        permits EventOutcome.Success, EventOutcome.Retry, EventOutcome.Fail, EventOutcome.Skip {

    /** Processing completed — the canonical {@link Success} instance. */
    static EventOutcome success() {
        return Success.INSTANCE;
    }

    /** Successful no-op, see {@link Skip}. */
    static EventOutcome skip(String reason) {
        return new Skip(reason);
    }

    /** Transient failure; the configured {@code FailureHandler} chain decides the delay. */
    static EventOutcome retry(String reason) {
        return new Retry(reason, null, null);
    }

    /** Transient failure carrying its cause; the chain decides the delay. */
    static EventOutcome retry(String reason, Throwable cause) {
        return new Retry(reason, null, cause);
    }

    /** Transient failure with an explicit delay before the next attempt. */
    static EventOutcome retry(String reason, Duration delay) {
        return new Retry(reason, delay, null);
    }

    /** Transient failure with an explicit delay and cause; either may be {@code null}. */
    static EventOutcome retry(String reason, @Nullable Duration delay, @Nullable Throwable cause) {
        return new Retry(reason, delay, cause);
    }

    /** Permanent failure — straight to {@code DISABLED}, see {@link Fail}. */
    static EventOutcome fail(String reason) {
        return new Fail(reason, null);
    }

    /** Permanent failure carrying its cause. */
    static EventOutcome fail(String reason, Throwable cause) {
        return new Fail(reason, cause);
    }

    /**
     * Processing completed successfully. The event is removed from the active outbox (or moved to
     * the archive if archiving is enabled).
     */
    record Success() implements EventOutcome {

        /** Canonical zero-state instance, returned by {@link EventOutcome#success()}. */
        public static final Success INSTANCE = new Success();
    }

    /**
     * Transient failure — re-schedule the event for another attempt.
     *
     * @param reason human-readable reason written to {@code last_fail_reason}
     * @param delayOverride optional explicit delay before the next attempt; if null, the configured
     *     {@code FailureHandler} chain decides (typically exponential backoff)
     * @param cause originating exception, or null if the handler returned {@code Retry} without one
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
     * path (typical idempotency check) and does not need to do any work. Treated as {@code Success}
     * by storage but distinguished in observability (separate listener callback, separate metric).
     *
     * @param reason human-readable reason written to logs; may be useful when investigating metrics
     *     later
     */
    record Skip(String reason) implements EventOutcome {

        public Skip {
            Objects.requireNonNull(reason, "reason must not be null");
        }
    }
}
