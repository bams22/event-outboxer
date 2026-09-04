/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bams22.outboxer.core.config.EventTypeConfig;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The thin merge documented in CONFIGURATION.md: a per-type override sets only the fields it
 * declares; everything else falls back to the resolved defaults, which in turn fall back to the
 * library defaults.
 */
class EventTypeThinMergeTest {

    @Test
    @DisplayName("empty properties object resolves to the library defaults unchanged")
    void emptyOverride_keepsBase() {
        EventTypeConfig base = EventTypeConfig.defaults();

        EventTypeConfig merged =
                OutboxEngineAutoConfiguration.mergeEventType(
                        new OutboxProperties.EventType(), base);

        assertThat(merged).isEqualTo(base);
    }

    @Test
    @DisplayName("override with a single field keeps every other field from the base")
    void singleFieldOverride_keepsOtherFields() {
        EventTypeConfig base = EventTypeConfig.defaults();
        OutboxProperties.EventType override = new OutboxProperties.EventType();
        override.setHandlerPoolSize(7);

        EventTypeConfig merged = OutboxEngineAutoConfiguration.mergeEventType(override, base);

        assertThat(merged.handlerPoolSize()).isEqualTo(7);
        assertThat(merged.pollMinInterval()).isEqualTo(base.pollMinInterval());
        assertThat(merged.pollMaxInterval()).isEqualTo(base.pollMaxInterval());
        assertThat(merged.pollMultiplier()).isEqualTo(base.pollMultiplier());
        assertThat(merged.claimBatchSize()).isEqualTo(base.claimBatchSize());
        assertThat(merged.claimMinFree()).isEqualTo(base.claimMinFree());
        assertThat(merged.handlerQueueCapacity()).isEqualTo(base.handlerQueueCapacity());
        assertThat(merged.handlerMaxRuntime()).isEqualTo(base.handlerMaxRuntime());
        assertThat(merged.interruptStuckHandler()).isEqualTo(base.interruptStuckHandler());
        assertThat(merged.lockTtl()).isEqualTo(base.lockTtl());
        assertThat(merged.lockWait()).isEqualTo(base.lockWait());
    }

    @Test
    @DisplayName("lock-wait overrides per type and falls back to the base (ADR-0035)")
    void lockWaitThinMerge() {
        EventTypeConfig base = EventTypeConfig.defaults();
        assertThat(base.lockWait()).isEqualTo(Duration.ofMillis(100));

        OutboxProperties.EventType override = new OutboxProperties.EventType();
        override.setLockWait(Duration.ZERO);

        EventTypeConfig merged = OutboxEngineAutoConfiguration.mergeEventType(override, base);
        assertThat(merged.lockWait()).isZero();
        assertThat(merged.lockTtl()).isEqualTo(base.lockTtl());
        assertThat(
                        OutboxEngineAutoConfiguration.mergeEventType(
                                        new OutboxProperties.EventType(), merged)
                                .lockWait())
                .isZero();
    }

    @Test
    @DisplayName("lock-wait at or above handler-max-runtime fails the merge")
    void lockWaitMustStayBelowHandlerMaxRuntime() {
        OutboxProperties.EventType override = new OutboxProperties.EventType();
        override.setHandlerMaxRuntime(Duration.ofSeconds(1));
        override.setLockWait(Duration.ofSeconds(1));

        assertThatThrownBy(
                        () ->
                                OutboxEngineAutoConfiguration.mergeEventType(
                                        override, EventTypeConfig.defaults()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lockWait");
    }

    @Test
    @DisplayName("interrupt-stuck-handler=false survives the merge; unset keeps the default true")
    void interruptStuckHandlerOptOut() {
        EventTypeConfig base = EventTypeConfig.defaults();
        assertThat(base.interruptStuckHandler()).isTrue();

        OutboxProperties.EventType override = new OutboxProperties.EventType();
        override.setInterruptStuckHandler(false);

        assertThat(
                        OutboxEngineAutoConfiguration.mergeEventType(override, base)
                                .interruptStuckHandler())
                .isFalse();
        assertThat(
                        OutboxEngineAutoConfiguration.mergeEventType(
                                        new OutboxProperties.EventType(), base)
                                .interruptStuckHandler())
                .isTrue();
    }

    @Test
    @DisplayName("per-type override layers on top of user defaults, not on library defaults")
    void overrideLayersOnUserDefaults() {
        OutboxProperties.EventType userDefaults = new OutboxProperties.EventType();
        userDefaults.setClaimBatchSize(42);
        userDefaults.setPollMinInterval(Duration.ofMillis(250));
        EventTypeConfig resolvedDefaults =
                OutboxEngineAutoConfiguration.mergeEventType(
                        userDefaults, EventTypeConfig.defaults());

        OutboxProperties.EventType override = new OutboxProperties.EventType();
        override.setHandlerPoolSize(9);
        EventTypeConfig merged =
                OutboxEngineAutoConfiguration.mergeEventType(override, resolvedDefaults);

        assertThat(merged.handlerPoolSize()).isEqualTo(9);
        assertThat(merged.claimBatchSize()).isEqualTo(42);
        assertThat(merged.pollMinInterval()).isEqualTo(Duration.ofMillis(250));
        assertThat(merged.pollMaxInterval())
                .isEqualTo(EventTypeConfig.defaults().pollMaxInterval());
    }

    @Test
    @DisplayName("claim-min-free merges like every other field and is bounded by pool + queue")
    void claimMinFreeMerge() {
        EventTypeConfig base = EventTypeConfig.defaults(); // pool 3 + queue 100
        OutboxProperties.EventType override = new OutboxProperties.EventType();
        override.setClaimMinFree(50);

        assertThat(OutboxEngineAutoConfiguration.mergeEventType(override, base).claimMinFree())
                .isEqualTo(50);

        OutboxProperties.EventType tooHigh = new OutboxProperties.EventType();
        tooHigh.setHandlerQueueCapacity(0);
        tooHigh.setClaimMinFree(4);
        assertThatThrownBy(() -> OutboxEngineAutoConfiguration.mergeEventType(tooHigh, base))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("claimMinFree");
    }

    @Test
    @DisplayName("platform executor warns when claim-min-free exceeds the queue capacity")
    void refillThresholdWarning() {
        EventTypeConfig fine =
                EventTypeConfig.defaults().toBuilder()
                        .handlerPoolSize(3)
                        .handlerQueueCapacity(30)
                        .claimMinFree(30)
                        .build();
        assertThat(OutboxEngineAutoConfiguration.refillThresholdWarning("T", fine)).isEmpty();

        EventTypeConfig idling = fine.toBuilder().claimMinFree(32).build();
        assertThat(OutboxEngineAutoConfiguration.refillThresholdWarning("T", idling))
                .hasValueSatisfying(
                        msg ->
                                assertThat(msg)
                                        .contains("'T'")
                                        .contains("claim-min-free (32)")
                                        .contains("handler-queue-capacity (30)")
                                        .contains("up to 2 of the 3"));
    }
}
