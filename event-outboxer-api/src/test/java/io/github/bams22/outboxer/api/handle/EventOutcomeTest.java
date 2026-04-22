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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class EventOutcomeTest {

  @Test
  void successInstanceIsCanonical() {
    assertThat(EventOutcome.Success.INSTANCE).isEqualTo(new EventOutcome.Success());
  }

  @Test
  void sealedInstanceofRoutingHitsCorrectBranch() {
    EventOutcome outcome = new EventOutcome.Retry("boom", Duration.ofSeconds(5), null);
    // Baseline is Java 17 (ADR-0017) — pattern-matching switch on sealed types is not
    // available. Use an instanceof chain with a terminating throw to keep exhaustiveness as a
    // runtime guarantee: adding a new EventOutcome subtype surfaces via the final else.
    String label;
    if (outcome instanceof EventOutcome.Success) {
      label = "success";
    } else if (outcome instanceof EventOutcome.Retry r) {
      label = "retry:" + r.reason();
    } else if (outcome instanceof EventOutcome.Fail f) {
      label = "fail:" + f.reason();
    } else if (outcome instanceof EventOutcome.Skip s) {
      label = "skip:" + s.reason();
    } else {
      throw new IllegalStateException("unhandled EventOutcome: " + outcome.getClass());
    }
    assertThat(label).isEqualTo("retry:boom");
  }

  @Test
  void retryAcceptsNullDelayOverrideAndCause() {
    EventOutcome.Retry r = new EventOutcome.Retry("reason", null, null);
    assertThat(r.delayOverride()).isNull();
    assertThat(r.cause()).isNull();
    assertThat(r.reason()).isEqualTo("reason");
  }

  @Test
  void failRequiresNonNullReason() {
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> new EventOutcome.Fail(null, null))
        .isInstanceOf(NullPointerException.class);
  }
}
