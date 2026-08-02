/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.api.handle.builtin;

import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.api.handle.FailureContext;
import io.github.bams22.outboxer.api.handle.FailureDecision;
import io.github.bams22.outboxer.api.handle.FailureHandler;

/**
 * Leaf handler that never retries: every failure goes straight to {@code DISABLED}. Useful for
 * handlers where retrying is pointless (for example, a validation step with deterministic
 * rejection).
 *
 * @param <T> payload type
 */
public final class NoRetryFailureHandler<T> implements FailureHandler<T> {

    @Override
    public FailureDecision onFailure(FailureContext<T> ctx) {
        String reason = reason(ctx);
        return new FailureDecision.Disable(reason);
    }

    private static String reason(FailureContext<?> ctx) {
        EventOutcome outcome = ctx.outcome();
        if (outcome != null) {
            return outcome.toString();
        }
        Throwable cause = ctx.cause();
        if (cause != null) {
            String m = cause.getMessage();
            return m == null ? cause.getClass().getSimpleName() : m;
        }
        return "no-retry policy";
    }
}
