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

import java.time.Instant;
import java.util.Objects;

/**
 * Decision produced by a {@link FailureHandler} chain in response to a failed attempt. Sealed —
 * exactly three decisions are possible: {@link RetryAt}, {@link Disable}, or {@link Delete}.
 *
 * <p>Unlike {@link EventOutcome}, this type is internal to the failure-handling chain: handlers
 * never return a {@code FailureDecision} directly; they return an {@code EventOutcome} which the
 * engine then runs through the configured {@code FailureHandler} to obtain one of these
 * decisions.
 */
public sealed interface FailureDecision permits
    FailureDecision.RetryAt, FailureDecision.Disable, FailureDecision.Delete {

  /**
   * Re-schedule the event to a specific wall-clock time. The engine writes {@code run_at =
   * when}, increments {@code attempts}, and sets the status back to {@code PENDING}.
   *
   * @param when next attempt time
   * @param reason human-readable reason written to {@code last_fail_reason}
   */
  record RetryAt(Instant when, String reason) implements FailureDecision {

    public RetryAt {
      Objects.requireNonNull(when, "when must not be null");
      Objects.requireNonNull(reason, "reason must not be null");
    }
  }

  /**
   * Move the event to {@code DISABLED}. Typical exhaustion outcome from {@code
   * MaxRetriesFailureHandler} or an explicit {@code Fail} from the handler.
   *
   * @param reason human-readable reason written to {@code last_fail_reason}
   */
  record Disable(String reason) implements FailureDecision {

    public Disable {
      Objects.requireNonNull(reason, "reason must not be null");
    }
  }

  /**
   * Permanently remove the event. Rarely used — mostly for workloads where post-mortem
   * inspection of failed events is not valuable.
   *
   * @param reason human-readable reason written to logs and metrics
   */
  record Delete(String reason) implements FailureDecision {

    public Delete {
      Objects.requireNonNull(reason, "reason must not be null");
    }
  }
}
