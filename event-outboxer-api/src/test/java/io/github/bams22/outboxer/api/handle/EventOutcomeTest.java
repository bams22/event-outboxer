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
  void sealedPatternMatchIsExhaustive() {
    EventOutcome outcome = new EventOutcome.Retry("boom", Duration.ofSeconds(5), null);
    // If a new subtype is added to EventOutcome, this switch fails to compile.
    String label =
        switch (outcome) {
          case EventOutcome.Success s -> "success";
          case EventOutcome.Retry r -> "retry:" + r.reason();
          case EventOutcome.Fail f -> "fail:" + f.reason();
          case EventOutcome.Skip s -> "skip:" + s.reason();
        };
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
