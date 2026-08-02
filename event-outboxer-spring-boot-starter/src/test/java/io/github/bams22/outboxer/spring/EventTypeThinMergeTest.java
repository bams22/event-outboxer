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
        assertThat(merged.handlerQueueCapacity()).isEqualTo(base.handlerQueueCapacity());
        assertThat(merged.handlerMaxRuntime()).isEqualTo(base.handlerMaxRuntime());
        assertThat(merged.lockTtl()).isEqualTo(base.lockTtl());
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
}
