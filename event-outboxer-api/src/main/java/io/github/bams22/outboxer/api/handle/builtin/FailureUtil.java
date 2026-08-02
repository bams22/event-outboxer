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

/**
 * Internal helpers shared by the built-in {@code FailureHandler} implementations. Package-private
 * on purpose — these are not part of the public API.
 */
final class FailureUtil {

  private FailureUtil() {}

  /**
   * Picks a human-readable reason string from a {@link FailureContext}, preferring the handler's
   * outcome reason (if any), then the exception message, then a generic fallback.
   */
  static String reason(FailureContext<?> ctx) {
    EventOutcome outcome = ctx.outcome();
    if (outcome instanceof EventOutcome.Retry r) {
      return r.reason();
    }
    if (outcome instanceof EventOutcome.Fail f) {
      return f.reason();
    }
    Throwable cause = ctx.cause();
    if (cause != null) {
      String m = cause.getMessage();
      return m == null ? cause.getClass().getSimpleName() : m;
    }
    return "handler returned a non-success outcome";
  }
}
