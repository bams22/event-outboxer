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

  /**
   * A randomness source pinned to the midpoint of {@code [0, 1)} makes the jitter factor exactly
   * 1.0, so the backoff assertions stay exact.
   */
  private static AdaptiveWaiter unjittered(Duration min, Duration max, double multiplier) {
    return new AdaptiveWaiter(min, max, multiplier, () -> 0.5);
  }

  @Test
  void startsAtMin() {
    AdaptiveWaiter w = unjittered(Duration.ofMillis(10), Duration.ofMillis(100), 2.0);
    assertThat(w.nextWait()).isEqualTo(Duration.ofMillis(10));
  }

  @Test
  void resetsToMinWhenWorkFound() {
    AdaptiveWaiter w = unjittered(Duration.ofMillis(10), Duration.ofMillis(100), 2.0);
    w.record(0);
    w.record(0);
    w.record(5);
    assertThat(w.nextWait()).isEqualTo(Duration.ofMillis(10));
  }

  @Test
  void growsMultiplicativelyOnEmpty() {
    AdaptiveWaiter w = unjittered(Duration.ofMillis(10), Duration.ofMillis(1000), 2.0);
    w.record(0);
    assertThat(w.nextWait()).isEqualTo(Duration.ofMillis(20));
    w.record(0);
    assertThat(w.nextWait()).isEqualTo(Duration.ofMillis(40));
  }

  @Test
  void capsAtMax() {
    AdaptiveWaiter w = unjittered(Duration.ofMillis(10), Duration.ofMillis(50), 10.0);
    w.record(0);
    assertThat(w.nextWait()).isEqualTo(Duration.ofMillis(50));
    w.record(0);
    assertThat(w.nextWait()).isEqualTo(Duration.ofMillis(50));
  }

  @Test
  void jitterStretchesTheWaitByUpToTenPercent() {
    AdaptiveWaiter low =
        new AdaptiveWaiter(Duration.ofMillis(100), Duration.ofSeconds(1), 2.0, () -> 0.0);
    assertThat(low.nextWait()).isEqualTo(Duration.ofMillis(90));

    AdaptiveWaiter high =
        new AdaptiveWaiter(Duration.ofMillis(100), Duration.ofSeconds(1), 2.0, () -> 1.0);
    assertThat(high.nextWait()).isEqualTo(Duration.ofMillis(110));
  }

  @Test
  void jitterAppliesOnEmissionWithoutAccumulating() {
    // A biased source shrinks every emitted wait, but the backoff state itself must keep
    // growing from the unjittered base — jitter must never compound across polls.
    AdaptiveWaiter w =
        new AdaptiveWaiter(Duration.ofMillis(100), Duration.ofSeconds(10), 2.0, () -> 0.0);
    w.record(0);
    assertThat(w.nextWait()).isEqualTo(Duration.ofMillis(180)); // 200ms base − 10%
    w.record(0);
    assertThat(w.nextWait()).isEqualTo(Duration.ofMillis(360)); // 400ms base − 10%
  }

  @Test
  void productionConstructorStaysWithinTenPercentOfBase() {
    AdaptiveWaiter w = new AdaptiveWaiter(Duration.ofMillis(1000), Duration.ofSeconds(10), 2.0);
    for (int i = 0; i < 1000; i++) {
      Duration wait = w.nextWait();
      assertThat(wait)
          .isGreaterThanOrEqualTo(Duration.ofMillis(900))
          .isLessThanOrEqualTo(Duration.ofMillis(1100));
    }
  }

  @Test
  void rejectsInvalidConfig() {
    assertThatThrownBy(() -> new AdaptiveWaiter(Duration.ZERO, Duration.ofMillis(100), 2.0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new AdaptiveWaiter(Duration.ofMillis(100), Duration.ofMillis(50), 2.0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new AdaptiveWaiter(Duration.ofMillis(10), Duration.ofMillis(100), 1.0))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
