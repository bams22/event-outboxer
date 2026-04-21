/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.polling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class AdaptiveWaiterTest {

  @Test
  void startsAtMin() {
    AdaptiveWaiter w =
        new AdaptiveWaiter(Duration.ofMillis(10), Duration.ofMillis(100), 2.0);
    assertThat(w.nextWait()).isEqualTo(Duration.ofMillis(10));
  }

  @Test
  void resetsToMinWhenWorkFound() {
    AdaptiveWaiter w =
        new AdaptiveWaiter(Duration.ofMillis(10), Duration.ofMillis(100), 2.0);
    w.record(0);
    w.record(0);
    w.record(5);
    assertThat(w.nextWait()).isEqualTo(Duration.ofMillis(10));
  }

  @Test
  void growsMultiplicativelyOnEmpty() {
    AdaptiveWaiter w =
        new AdaptiveWaiter(Duration.ofMillis(10), Duration.ofMillis(1000), 2.0);
    w.record(0);
    assertThat(w.nextWait()).isEqualTo(Duration.ofMillis(20));
    w.record(0);
    assertThat(w.nextWait()).isEqualTo(Duration.ofMillis(40));
  }

  @Test
  void capsAtMax() {
    AdaptiveWaiter w =
        new AdaptiveWaiter(Duration.ofMillis(10), Duration.ofMillis(50), 10.0);
    w.record(0);
    assertThat(w.nextWait()).isEqualTo(Duration.ofMillis(50));
    w.record(0);
    assertThat(w.nextWait()).isEqualTo(Duration.ofMillis(50));
  }

  @Test
  void rejectsInvalidConfig() {
    assertThatThrownBy(
            () -> new AdaptiveWaiter(Duration.ZERO, Duration.ofMillis(100), 2.0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> new AdaptiveWaiter(Duration.ofMillis(100), Duration.ofMillis(50), 2.0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> new AdaptiveWaiter(Duration.ofMillis(10), Duration.ofMillis(100), 1.0))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
