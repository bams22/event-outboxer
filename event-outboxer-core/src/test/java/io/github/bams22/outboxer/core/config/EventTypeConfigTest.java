/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class EventTypeConfigTest {

    @Test
    void defaultsAreValid() {
        EventTypeConfig d = EventTypeConfig.defaults();
        assertThat(d.pollMinInterval()).isPositive();
        assertThat(d.pollMaxInterval()).isGreaterThanOrEqualTo(d.pollMinInterval());
        assertThat(d.pollMultiplier()).isGreaterThan(1.0);
        assertThat(d.claimBatchSize()).isPositive();
        assertThat(d.handlerPoolSize()).isPositive();
        assertThat(d.handlerMaxRuntime()).isPositive();
    }

    @Test
    void rejectsInvalidFields() {
        assertThatThrownBy(() -> EventTypeConfig.defaults().toBuilder().claimBatchSize(0).build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EventTypeConfig.defaults().toBuilder().pollMultiplier(1.0).build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                EventTypeConfig.defaults().toBuilder()
                                        .handlerMaxRuntime(Duration.ZERO)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void lockTtlMustCoverHandlerMaxRuntime() {
        // Shorter TTL than the handler budget → the entity lock could expire mid-handler.
        assertThatThrownBy(
                        () ->
                                EventTypeConfig.defaults().toBuilder()
                                        .handlerMaxRuntime(Duration.ofMinutes(10))
                                        .lockTtl(Duration.ofMinutes(5))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lockTtl");
        // Equality is the minimum allowed; the default keeps a 2x margin.
        EventTypeConfig equal =
                EventTypeConfig.defaults().toBuilder()
                        .handlerMaxRuntime(Duration.ofMinutes(10))
                        .lockTtl(Duration.ofMinutes(10))
                        .build();
        assertThat(equal.lockTtl()).isEqualTo(equal.handlerMaxRuntime());
        assertThat(EventTypeConfig.defaults().lockTtl())
                .isEqualTo(EventTypeConfig.defaults().handlerMaxRuntime().multipliedBy(2));
    }

    @Test
    void providerReturnsOverrideWhenPresent() {
        EventTypeConfig alt = EventTypeConfig.defaults().toBuilder().claimBatchSize(99).build();
        EventTypeConfigProvider provider =
                new EventTypeConfigProvider(
                        EventTypeConfig.defaults(), java.util.Map.of("type-alt", alt));

        assertThat(provider.forType("type-default")).isEqualTo(EventTypeConfig.defaults());
        assertThat(provider.forType("type-alt")).isEqualTo(alt);
    }
}
