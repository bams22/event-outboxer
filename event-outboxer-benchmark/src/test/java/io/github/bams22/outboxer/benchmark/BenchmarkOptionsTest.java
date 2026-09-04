/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bams22.outboxer.benchmark.scenario.ExecutorType;
import io.github.bams22.outboxer.benchmark.scenario.FleetMode;
import io.github.bams22.outboxer.benchmark.scenario.LockType;
import io.github.bams22.outboxer.benchmark.scenario.PostgresRestart;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class BenchmarkOptionsTest {

    @Test
    void noArgumentsMeansSmokeOnADisposableDatabase() {
        BenchmarkOptions o = BenchmarkOptions.parse();

        assertThat(o.scenario().name()).isEqualTo("smoke");
        assertThat(o.targetName()).isEqualTo("outboxer");
        assertThat(o.database()).isNull();
        assertThat(o.postgresImage()).isEqualTo("postgres:15");
        assertThat(o.reportDir()).isEqualTo(Path.of("target", "bench"));
    }

    @Test
    void presetKnobsCanBeOverriddenByName() {
        BenchmarkOptions o =
                BenchmarkOptions.parse(
                        "--bench.scenario=hot-key",
                        "--bench.workers=5",
                        "--bench.lock=noop",
                        "--bench.executor=virtual",
                        "--bench.poll-min-interval=50ms",
                        "--bench.handler-work-time=2ms",
                        "--bench.finalize-batching=off",
                        "--bench.failure-rate=0.25",
                        "--bench.workers-after-publish=true");

        var s = o.scenario();
        assertThat(s.name()).isEqualTo("hot-key");
        assertThat(s.lockKeyCardinality()).isEqualTo(8);
        assertThat(s.workers()).isEqualTo(5);
        assertThat(s.lockType()).isEqualTo(LockType.NOOP);
        assertThat(s.executorType()).isEqualTo(ExecutorType.VIRTUAL);
        assertThat(s.pollMinInterval()).isEqualTo(Duration.ofMillis(50));
        assertThat(s.handlerWorkTime()).isEqualTo(Duration.ofMillis(2));
        assertThat(s.finalizeBatching()).isFalse();
        assertThat(s.failureRate()).isEqualTo(0.25);
        assertThat(s.workersStartAfterPublish()).isTrue();
    }

    @Test
    void fleetAndChaosKnobsOverrideThePreset() {
        BenchmarkOptions o =
                BenchmarkOptions.parse(
                        "--bench.scenario=crash",
                        "--bench.kill-workers=1",
                        "--bench.kill-at=0.5",
                        "--bench.respawn-killed=false",
                        "--bench.pg-restart=crash",
                        "--bench.pg-restart-at=0.7",
                        "--bench.worker-jvm-args=-Xmx256m -XX:+UseSerialGC");

        var s = o.scenario();
        assertThat(s.fleet()).isEqualTo(FleetMode.FORKED);
        assertThat(s.chaos().killWorkers()).isEqualTo(1);
        assertThat(s.chaos().killAtProgress()).isEqualTo(0.5);
        assertThat(s.chaos().respawnKilled()).isFalse();
        assertThat(s.chaos().postgresRestart()).isEqualTo(PostgresRestart.CRASH);
        assertThat(s.chaos().postgresRestartAtProgress()).isEqualTo(0.7);
        assertThat(s.workerJvmArgs()).containsExactly("-Xmx256m", "-XX:+UseSerialGC");
    }

    @Test
    void forkedFleetWithoutChaosIsJustAFleetChoice() {
        BenchmarkOptions o =
                BenchmarkOptions.parse("--bench.scenario=throughput", "--bench.fleet=forked");
        assertThat(o.scenario().fleet()).isEqualTo(FleetMode.FORKED);
        assertThat(o.scenario().chaos().any()).isFalse();
    }

    @Test
    void workerPropertiesPassThroughAndMergeWithThePreset() {
        BenchmarkOptions o =
                BenchmarkOptions.parse(
                        "--bench.scenario=failures",
                        "--bench.worker-prop.event-outboxer.maintenance.heartbeat-interval=1s");

        assertThat(o.scenario().workerProperties())
                .containsEntry("event-outboxer.maintenance.heartbeat-interval", "1s")
                .containsEntry("event-outboxer.event-types.defaults.failure.base-delay", "200ms");
    }

    @Test
    void externalDatabaseIsBuiltFromJdbcOptions() {
        BenchmarkOptions o =
                BenchmarkOptions.parse(
                        "--bench.jdbc-url=jdbc:postgresql://db:5432/bench",
                        "--bench.jdbc-user=u",
                        "--bench.jdbc-password=p",
                        "--bench.report-dir=/tmp/reports");

        assertThat(o.database()).isNotNull();
        assertThat(o.database().jdbcUrl()).isEqualTo("jdbc:postgresql://db:5432/bench");
        assertThat(o.database().username()).isEqualTo("u");
        assertThat(o.database().password()).isEqualTo("p");
        assertThat(o.reportDir()).isEqualTo(Path.of("/tmp/reports"));
    }

    @Test
    void unknownKeyIsRejectedWithTheKnownList() {
        assertThatThrownBy(() -> BenchmarkOptions.parse("--bench.wrokers=3"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("wrokers")
                .hasMessageContaining("workers");
    }

    @Test
    void argumentsOutsideThePrefixAreIgnored() {
        BenchmarkOptions o =
                BenchmarkOptions.parse("--spring.profiles.active=x", "--bench.events=7");
        assertThat(o.scenario().events()).isEqualTo(7);
    }
}
