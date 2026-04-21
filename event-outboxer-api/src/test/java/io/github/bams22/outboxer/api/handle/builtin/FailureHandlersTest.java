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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.api.handle.FailureContext;
import io.github.bams22.outboxer.api.handle.FailureDecision;
import io.github.bams22.outboxer.api.handle.FailureHandler;
import io.github.bams22.outboxer.domain.ClaimedEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class FailureHandlersTest {

  private static final Instant NOW = Instant.parse("2026-04-21T12:00:00Z");

  private static FailureContext<String> ctx(int attempt, EventOutcome outcome, Throwable cause) {
    ClaimedEvent event =
        new ClaimedEvent(
            UUID.randomUUID(),
            "TEST",
            "{}",
            "java.lang.String",
            (short) 0,
            attempt,
            NOW.minusSeconds(10),
            NOW.minusSeconds(1),
            Map.of(),
            1L);
    return new FailureContext<>(event, "payload", outcome, cause, attempt, NOW);
  }

  @Test
  void noRetryAlwaysDisables() {
    FailureDecision d =
        new NoRetryFailureHandler<String>()
            .onFailure(ctx(1, new EventOutcome.Retry("x", null, null), null));
    assertThat(d).isInstanceOf(FailureDecision.Disable.class);
  }

  @Test
  void fixedDelayReturnsNowPlusDelay() {
    Duration delay = Duration.ofSeconds(42);
    FailureDecision d =
        new FixedDelayFailureHandler<String>(delay)
            .onFailure(ctx(1, new EventOutcome.Retry("x", null, null), null));
    assertThat(d).isInstanceOf(FailureDecision.RetryAt.class);
    assertThat(((FailureDecision.RetryAt) d).when()).isEqualTo(NOW.plus(delay));
  }

  @Test
  void fixedDelayHonorsDelayOverrideFromOutcome() {
    Duration override = Duration.ofMillis(250);
    FailureHandler<String> h = new FixedDelayFailureHandler<>(Duration.ofSeconds(10));
    FailureDecision d = h.onFailure(ctx(1, new EventOutcome.Retry("x", override, null), null));
    assertThat(((FailureDecision.RetryAt) d).when()).isEqualTo(NOW.plus(override));
  }

  @Test
  void fixedDelayRejectsNonPositive() {
    assertThatThrownBy(() -> new FixedDelayFailureHandler<String>(Duration.ZERO))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new FixedDelayFailureHandler<String>(Duration.ofSeconds(-1)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void exponentialBackoffGrowsAndRespectsCap() {
    ExponentialBackoffFailureHandler<String> h =
        new ExponentialBackoffFailureHandler<>(
            Duration.ofSeconds(1), 2.0, Duration.ofSeconds(30), 0.0);

    Duration d1 = delayBetween(h.onFailure(ctx(1, null, new RuntimeException("x"))));
    Duration d2 = delayBetween(h.onFailure(ctx(2, null, new RuntimeException("x"))));
    Duration d3 = delayBetween(h.onFailure(ctx(3, null, new RuntimeException("x"))));
    Duration dBig = delayBetween(h.onFailure(ctx(20, null, new RuntimeException("x"))));

    // attempt 1 → 1s, 2 → 2s, 3 → 4s (multiplier=2, no jitter).
    assertThat(d1).isEqualTo(Duration.ofSeconds(1));
    assertThat(d2).isEqualTo(Duration.ofSeconds(2));
    assertThat(d3).isEqualTo(Duration.ofSeconds(4));
    // very high attempt count clamps at cap.
    assertThat(dBig).isEqualTo(Duration.ofSeconds(30));
  }

  @Test
  void exponentialBackoffJitterIsBoundedByConfig() {
    ExponentialBackoffFailureHandler<String> h =
        new ExponentialBackoffFailureHandler<>(
            Duration.ofSeconds(10), 2.0, Duration.ofHours(1), 0.5);

    long baseMillis = Duration.ofSeconds(10).toMillis();
    long min = (long) (baseMillis * 0.5);
    long max = (long) (baseMillis * 1.5);
    for (int i = 0; i < 100; i++) {
      Duration d = delayBetween(h.onFailure(ctx(1, null, new RuntimeException("x"))));
      assertThat(d.toMillis()).isBetween(min, max);
    }
  }

  @Test
  void exponentialBackoffValidatesParams() {
    assertThatThrownBy(
            () ->
                new ExponentialBackoffFailureHandler<String>(
                    Duration.ofSeconds(1), 1.0, Duration.ofHours(1), 0.0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("multiplier");
    assertThatThrownBy(
            () ->
                new ExponentialBackoffFailureHandler<String>(
                    Duration.ofSeconds(10), 2.0, Duration.ofSeconds(1), 0.0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cap");
    assertThatThrownBy(
            () ->
                new ExponentialBackoffFailureHandler<String>(
                    Duration.ofSeconds(1), 2.0, Duration.ofHours(1), 1.5))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("jitter");
  }

  @Test
  void maxRetriesDelegatesBelowCap() {
    FailureHandler<String> leaf = new FixedDelayFailureHandler<>(Duration.ofSeconds(5));
    FailureHandler<String> capped =
        new MaxRetriesFailureHandler<>(5, MaxRetriesFailureHandler.ExhaustedAction.DISABLE, leaf);

    FailureDecision d = capped.onFailure(ctx(2, new EventOutcome.Retry("x", null, null), null));
    assertThat(d).isInstanceOf(FailureDecision.RetryAt.class);
  }

  @Test
  void maxRetriesExhaustsAtCapWithDisableByDefault() {
    FailureHandler<String> leaf = new FixedDelayFailureHandler<>(Duration.ofSeconds(5));
    FailureHandler<String> capped =
        new MaxRetriesFailureHandler<>(3, MaxRetriesFailureHandler.ExhaustedAction.DISABLE, leaf);

    FailureDecision d = capped.onFailure(ctx(3, new EventOutcome.Retry("x", null, null), null));
    assertThat(d).isInstanceOf(FailureDecision.Disable.class);
  }

  @Test
  void maxRetriesExhaustsWithDeleteWhenConfigured() {
    FailureHandler<String> leaf = new FixedDelayFailureHandler<>(Duration.ofSeconds(5));
    FailureHandler<String> capped =
        new MaxRetriesFailureHandler<>(3, MaxRetriesFailureHandler.ExhaustedAction.DELETE, leaf);

    FailureDecision d = capped.onFailure(ctx(3, new EventOutcome.Retry("x", null, null), null));
    assertThat(d).isInstanceOf(FailureDecision.Delete.class);
  }

  @Test
  void maxRetriesShortCircuitsOnExplicitFail() {
    FailureHandler<String> leaf = new FixedDelayFailureHandler<>(Duration.ofSeconds(5));
    FailureHandler<String> capped =
        new MaxRetriesFailureHandler<>(10, MaxRetriesFailureHandler.ExhaustedAction.DISABLE, leaf);

    FailureDecision d = capped.onFailure(ctx(1, new EventOutcome.Fail("permanent", null), null));
    assertThat(d).isInstanceOf(FailureDecision.Disable.class);
  }

  @Test
  void defaultsChainProducesRetryOnFirstFailure() {
    FailureHandler<String> chain = FailureHandlers.defaults();
    FailureDecision d = chain.onFailure(ctx(1, null, new RuntimeException("transient")));
    assertThat(d).isInstanceOf(FailureDecision.RetryAt.class);
  }

  @Test
  void defaultsChainDisablesAtTenthAttempt() {
    FailureHandler<String> chain = FailureHandlers.defaults();
    FailureDecision d = chain.onFailure(ctx(10, null, new RuntimeException("transient")));
    assertThat(d).isInstanceOf(FailureDecision.Disable.class);
  }

  private static Duration delayBetween(FailureDecision d) {
    FailureDecision.RetryAt r = (FailureDecision.RetryAt) d;
    return Duration.between(NOW, r.when());
  }
}
