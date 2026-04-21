/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class SettableClockTest {

  @Test
  void initialValueReadsBack() {
    Instant start = Instant.parse("2026-04-21T12:00:00Z");
    SettableClock clock = new SettableClock(start);
    assertThat(clock.now()).isEqualTo(start);
  }

  @Test
  void advanceMovesForward() {
    SettableClock clock = SettableClock.atEpoch();
    clock.advance(Duration.ofSeconds(10));
    assertThat(clock.now()).isEqualTo(Instant.EPOCH.plusSeconds(10));
  }

  @Test
  void setReplacesCurrent() {
    SettableClock clock = SettableClock.atEpoch();
    Instant target = Instant.parse("2030-01-01T00:00:00Z");
    clock.set(target);
    assertThat(clock.now()).isEqualTo(target);
  }
}
