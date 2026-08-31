/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.maintenance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bams22.outboxer.api.observer.HeartbeatFailedInfo;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.observer.WorkerRegisteredInfo;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.domain.WorkerInfo;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.WorkerRegistry;
import io.github.bams22.outboxer.storage.inmemory.InMemoryWorkerRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Heartbeat semantics around the {@code lastSuccessAt} seam and failure propagation: successes
 * (including the re-register recovery path) advance the timestamp; failures fire {@code
 * onHeartbeatFailed} and then propagate to the scheduler's guard.
 */
class HeartbeatTaskTest {

    private static final WorkerInfo WORKER =
            WorkerInfo.builder()
                    .id(new WorkerId("hb-1"))
                    .host("test-host")
                    .metadata(Map.of())
                    .build();

    private final AtomicReference<Instant> now =
            new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));
    private final Clock clock = now::get;

    @Test
    @DisplayName("successful heartbeat advances lastSuccessAt")
    void successAdvancesLastSuccessAt() {
        InMemoryWorkerRegistry registry = new InMemoryWorkerRegistry();
        registry.register(WORKER);
        HeartbeatTask task = new HeartbeatTask(registry, WORKER, clock, OutboxListener.NOOP);
        assertThat(task.lastSuccessAt()).isNull();

        task.run();

        assertThat(task.lastSuccessAt()).isEqualTo(now.get());

        now.updateAndGet(t -> t.plus(Duration.ofSeconds(5)));
        task.run();
        assertThat(task.lastSuccessAt()).isEqualTo(now.get());
    }

    @Test
    @DisplayName("reaped worker row → re-register counts as a success")
    void missingRowReRegistersAndCountsAsSuccess() {
        InMemoryWorkerRegistry registry = new InMemoryWorkerRegistry();
        List<WorkerRegisteredInfo> registered = new ArrayList<>();
        OutboxListener listener =
                new OutboxListener() {
                    @Override
                    public void onWorkerRegistered(WorkerRegisteredInfo info) {
                        registered.add(info);
                    }
                };
        HeartbeatTask task = new HeartbeatTask(registry, WORKER, clock, listener);

        task.run();

        assertThat(registry.findById(WORKER.id())).isPresent();
        assertThat(registered).hasSize(1);
        assertThat(task.lastSuccessAt()).isEqualTo(now.get());
    }

    @Test
    @DisplayName("failure fires onHeartbeatFailed, propagates, and leaves lastSuccessAt untouched")
    void failureFiresListenerAndPropagates() {
        List<HeartbeatFailedInfo> failed = new ArrayList<>();
        OutboxListener listener =
                new OutboxListener() {
                    @Override
                    public void onHeartbeatFailed(HeartbeatFailedInfo info) {
                        failed.add(info);
                    }
                };
        HeartbeatTask task = new HeartbeatTask(new ThrowingRegistry(), WORKER, clock, listener);

        assertThatThrownBy(task::run)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("registry down");

        assertThat(failed)
                .singleElement()
                .satisfies(
                        info -> {
                            assertThat(info.workerId()).isEqualTo(WORKER.id());
                            assertThat(info.cause()).hasMessageContaining("registry down");
                        });
        assertThat(task.lastSuccessAt()).isNull();
    }

    /** Registry whose heartbeat always fails — simulates a DB outage. */
    private static final class ThrowingRegistry implements WorkerRegistry {
        @Override
        public void register(WorkerInfo info) {}

        @Override
        public boolean heartbeat(WorkerId id, Instant at) {
            throw new IllegalStateException("registry down (simulated)");
        }

        @Override
        public void markGracefulStop(WorkerId id) {}

        @Override
        public void deregister(WorkerId id) {}

        @Override
        public List<WorkerInfo> findDead(Duration deadThreshold, int limit) {
            return List.of();
        }

        @Override
        public void removeDead(List<WorkerId> ids) {}

        @Override
        public java.util.Optional<WorkerInfo> findById(WorkerId id) {
            return java.util.Optional.empty();
        }

        @Override
        public List<WorkerInfo> findAll() {
            return List.of();
        }
    }
}
