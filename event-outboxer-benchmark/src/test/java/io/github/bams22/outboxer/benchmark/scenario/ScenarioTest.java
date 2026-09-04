/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ScenarioTest {

    @ParameterizedTest
    @ValueSource(strings = {"smoke", "throughput", "hot-key", "failures", "backlog"})
    void everyPresetBuildsAndCarriesItsName(String name) {
        Scenario s = Scenario.preset(name);
        assertThat(s.name()).isEqualTo(name);
        assertThat(Scenario.PRESETS).contains(name);
    }

    @Test
    void blankPresetNameMeansSmoke() {
        assertThat(Scenario.preset(null).name()).isEqualTo("smoke");
        assertThat(Scenario.preset("  ").name()).isEqualTo("smoke");
    }

    @Test
    void unknownPresetListsTheKnownOnes() {
        assertThatThrownBy(() -> Scenario.preset("nope"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("smoke")
                .hasMessageContaining("backlog");
    }

    @Test
    void unsetKnobsTakeHarnessDefaults() {
        Scenario s = Scenario.builder().name("x").events(10).build();

        assertThat(s.eventTypes()).isEqualTo(1);
        assertThat(s.workers()).isEqualTo(2);
        assertThat(s.handlerPoolSize()).isEqualTo(3);
        assertThat(s.claimBatchSize()).isEqualTo(10);
        assertThat(s.pollMinInterval()).isEqualTo(Duration.ofMillis(100));
        assertThat(s.pollMaxInterval()).isEqualTo(Duration.ofSeconds(1));
        assertThat(s.executorType()).isEqualTo(ExecutorType.PLATFORM);
        assertThat(s.lockType()).isEqualTo(LockType.NOOP);
        assertThat(s.finalizeBatching()).isTrue();
        assertThat(s.handlerWorkTime()).isZero();
        assertThat(s.failureRate()).isZero();
        assertThat(s.workerProperties()).isEmpty();
    }

    @Test
    void invalidValuesAreRejected() {
        assertThatThrownBy(() -> Scenario.builder().name("x").events(0).build())
                .hasMessageContaining("events");
        assertThatThrownBy(() -> Scenario.builder().name("x").events(1).failureRate(1.5).build())
                .hasMessageContaining("failureRate");
        assertThatThrownBy(
                        () ->
                                Scenario.builder()
                                        .name("x")
                                        .events(1)
                                        .pollMinInterval(Duration.ofSeconds(2))
                                        .pollMaxInterval(Duration.ofSeconds(1))
                                        .build())
                .hasMessageContaining("pollMaxInterval");
        assertThatThrownBy(() -> Scenario.builder().name(" ").events(1).build())
                .hasMessageContaining("name");
    }

    @Test
    void lockKeysAndTypesAreSpreadRoundRobin() {
        Scenario s =
                Scenario.builder().name("x").events(10).eventTypes(3).lockKeyCardinality(4).build();

        assertThat(s.lockKeyFor(0)).isEqualTo("key-0");
        assertThat(s.lockKeyFor(5)).isEqualTo("key-1");
        assertThat(s.typeIndexFor(4)).isEqualTo(1);
        assertThat(Scenario.builder().name("x").events(1).build().lockKeyFor(9)).isNull();
    }

    @Test
    void toBuilderKeepsEverythingElse() {
        Scenario hot = Scenario.hotKey();
        Scenario tweaked = hot.toBuilder().lockType(LockType.NOOP).build();

        assertThat(tweaked.lockKeyCardinality()).isEqualTo(hot.lockKeyCardinality());
        assertThat(tweaked.lockType()).isEqualTo(LockType.NOOP);
        assertThat(tweaked.lockType().exclusive()).isFalse();
    }
}
