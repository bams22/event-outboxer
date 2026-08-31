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
import static org.awaitility.Awaitility.await;

import io.github.bams22.outboxer.api.observer.MaintenanceRunInfo;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.core.config.MaintenanceConfig;
import io.github.bams22.outboxer.core.dispatch.InFlightRegistry;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.domain.WorkerInfo;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.WorkerRegistry;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryWorkerRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The scheduler's guarded wrapper: every task run — successful or failed — is reported through
 * {@code onMaintenanceRunCompleted}, and neither a failing task nor a throwing listener cancels the
 * task's schedule (the {@code scheduleWithFixedDelay} cancel-on-throw regression guard).
 */
class MaintenanceSchedulerTest {

    private static final WorkerInfo WORKER =
            WorkerInfo.builder()
                    .id(new WorkerId("mnt-1"))
                    .host("test-host")
                    .metadata(Map.of())
                    .build();

    private final CopyOnWriteArrayList<MaintenanceRunInfo> runs = new CopyOnWriteArrayList<>();
    private MaintenanceScheduler scheduler;

    @AfterEach
    void stopScheduler() {
        if (scheduler != null) {
            scheduler.stop(Duration.ofSeconds(5));
        }
    }

    @Test
    @DisplayName("successful runs are reported with result=OK per task name")
    void okRunsAreReported() {
        InMemoryWorkerRegistry registry = new InMemoryWorkerRegistry();
        registry.register(WORKER);
        scheduler = scheduler(registry, runs::add);

        scheduler.start();

        await().atMost(Duration.ofSeconds(5))
                .until(() -> runs.stream().filter(r -> r.task().equals("heartbeat")).count() >= 2);
        assertThat(runs)
                .filteredOn(r -> r.task().equals("heartbeat"))
                .allSatisfy(
                        r -> {
                            assertThat(r.result()).isEqualTo(MaintenanceRunInfo.Result.OK);
                            assertThat(r.cause()).isNull();
                        });
    }

    @Test
    @DisplayName("a failing task reports result=FAILED with the cause and keeps its schedule alive")
    void failedRunsKeepTicking() {
        scheduler = scheduler(new ThrowingRegistry(), runs::add);

        scheduler.start();

        await().atMost(Duration.ofSeconds(5))
                .until(
                        () ->
                                runs.stream()
                                                .filter(
                                                        r ->
                                                                r.task().equals("heartbeat")
                                                                        && r.result()
                                                                                == MaintenanceRunInfo
                                                                                        .Result
                                                                                        .FAILED)
                                                .count()
                                        >= 2);
        assertThat(runs)
                .filteredOn(r -> r.task().equals("heartbeat"))
                .allSatisfy(r -> assertThat(r.cause()).hasMessageContaining("registry down"));
    }

    @Test
    @DisplayName("a throwing listener does not cancel the task's schedule")
    void throwingListenerDoesNotKillSchedule() {
        ThrowingRegistry registry = new ThrowingRegistry();
        scheduler =
                scheduler(
                        registry,
                        info -> {
                            throw new IllegalStateException("listener exploded");
                        });

        scheduler.start();

        await().atMost(Duration.ofSeconds(5)).until(() -> registry.heartbeats.get() >= 2);
    }

    private MaintenanceScheduler scheduler(
            WorkerRegistry registry, java.util.function.Consumer<MaintenanceRunInfo> onRun) {
        InMemoryEventStore store = new InMemoryEventStore();
        OutboxListener listener =
                new OutboxListener() {
                    @Override
                    public void onMaintenanceRunCompleted(MaintenanceRunInfo info) {
                        onRun.accept(info);
                    }
                };
        Duration never = Duration.ofHours(1);
        return MaintenanceScheduler.builder()
                .heartbeat(new HeartbeatTask(registry, WORKER, Clock.system(), OutboxListener.NOOP))
                .orphanRecovery(
                        OrphanRecoveryTask.builder().registry(registry).store(store).build())
                .watchdog(
                        WatchdogTask.builder()
                                .inFlight(new InFlightRegistry())
                                .store(store)
                                .build())
                .engineHealthCheck(new EngineHealthCheckTask(List.of(), (reason, cause) -> {}))
                .staleClaimSweeper(
                        StaleClaimSweeperTask.builder()
                                .store(store)
                                .threshold(Duration.ofMinutes(10))
                                .interval(never)
                                .build())
                .config(
                        MaintenanceConfig.builder()
                                .heartbeatInterval(Duration.ofMillis(20))
                                .deadThreshold(Duration.ofMillis(60))
                                .orphanRecoveryInterval(never)
                                .watchdogInterval(never)
                                .abandonedHandlerGrace(Duration.ofMinutes(1))
                                .reclaimBatchSize(100)
                                .shutdownTimeout(Duration.ofSeconds(5))
                                .staleClaimSweepInterval(never)
                                .build())
                .listener(listener)
                .build();
    }

    /** Registry whose heartbeat always fails, counting the attempts — simulates a DB outage. */
    private static final class ThrowingRegistry implements WorkerRegistry {
        final AtomicInteger heartbeats = new AtomicInteger();

        @Override
        public void register(WorkerInfo info) {}

        @Override
        public boolean heartbeat(WorkerId id, Instant at) {
            heartbeats.incrementAndGet();
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
        public Optional<WorkerInfo> findById(WorkerId id) {
            return Optional.empty();
        }

        @Override
        public List<WorkerInfo> findAll() {
            return List.of();
        }
    }
}
