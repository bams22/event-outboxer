/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spi.contracts;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.domain.WorkerInfo;
import io.github.bams22.outboxer.spi.WorkerRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Reusable contract specification for every {@link WorkerRegistry} implementation. Focuses on the
 * interactions used by the orphan-recovery task: register, heartbeat, findDead, removeDead — see
 * ADR-0005 and STORAGE.md §Tables / workers.
 */
public abstract class AbstractWorkerRegistryContractTest {

  protected WorkerRegistry registry;

  protected abstract WorkerRegistry newRegistry();

  /**
   * Force the stored heartbeat of {@code id} to the (past) instant {@code at}. Adapters that stamp
   * heartbeats with their own time source — the PostgreSQL adapter uses the database clock,
   * ignoring the {@code at} argument of {@code heartbeat()} — must override this with a direct
   * write so the staleness scenarios below can be simulated.
   */
  protected void backdateHeartbeat(WorkerId id, Instant at) {
    registry.heartbeat(id, at);
  }

  @BeforeEach
  void setUpRegistry() {
    registry = newRegistry();
  }

  @Test
  @DisplayName("register() persists the worker and findById() returns it")
  void register_roundTrip() {
    WorkerId id = new WorkerId("w-1");
    WorkerInfo info = workerInfo(id, "host-1");

    registry.register(info);

    Optional<WorkerInfo> found = registry.findById(id);
    assertThat(found).isPresent();
    assertThat(found.orElseThrow().id()).isEqualTo(id);
    assertThat(found.orElseThrow().host()).isEqualTo("host-1");
  }

  @Test
  @DisplayName("register() with an existing id overwrites the previous record")
  void register_overwrite() {
    WorkerId id = new WorkerId("w-1");
    registry.register(workerInfo(id, "host-old"));

    registry.register(workerInfo(id, "host-new"));

    assertThat(registry.findById(id).orElseThrow().host()).isEqualTo("host-new");
  }

  @Test
  @DisplayName("findById() returns empty when no row exists")
  void findById_empty() {
    assertThat(registry.findById(new WorkerId("unknown"))).isEmpty();
  }

  @Test
  @DisplayName("findAll() returns every registered worker")
  void findAll_returnsEveryWorker() {
    registry.register(workerInfo(new WorkerId("w-1"), "h-1"));
    registry.register(workerInfo(new WorkerId("w-2"), "h-2"));
    registry.register(workerInfo(new WorkerId("w-3"), "h-3"));

    assertThat(registry.findAll())
        .extracting(w -> w.id().value())
        .containsExactlyInAnyOrder("w-1", "w-2", "w-3");
  }

  @Test
  @DisplayName("heartbeat() returns true for a known worker, false for an unknown one")
  void heartbeat_returnsWhetherUpdated() {
    WorkerId known = new WorkerId("w-known");
    registry.register(workerInfo(known, "host"));

    assertThat(registry.heartbeat(known, Instant.now())).isTrue();
    assertThat(registry.heartbeat(new WorkerId("w-missing"), Instant.now())).isFalse();
  }

  @Test
  @DisplayName("findDead() returns only workers whose heartbeat is older than the threshold")
  void findDead_returnsStaleWorkers() {
    Instant now = Instant.now();
    WorkerId fresh = new WorkerId("fresh");
    WorkerId stale = new WorkerId("stale");
    registry.register(workerInfo(fresh, "host-fresh"));
    registry.register(workerInfo(stale, "host-stale"));

    registry.heartbeat(fresh, now);
    backdateHeartbeat(stale, now.minus(Duration.ofMinutes(10)));

    List<WorkerInfo> dead = registry.findDead(Duration.ofMinutes(1), 100);

    assertThat(dead).extracting(w -> w.id().value()).containsExactly("stale");
  }

  @Test
  @DisplayName(
      "findDead() includes graceful-stop workers immediately, without waiting for the threshold")
  void findDead_includesGracefulStopWorkers() {
    WorkerId fresh = new WorkerId("fresh");
    WorkerId stopping = new WorkerId("stopping");
    registry.register(workerInfo(fresh, "h-fresh"));
    registry.register(workerInfo(stopping, "h-stopping"));

    registry.markGracefulStop(stopping);

    List<WorkerInfo> dead = registry.findDead(Duration.ofMinutes(10), 100);

    assertThat(dead).extracting(w -> w.id().value()).containsExactly("stopping");
  }

  @Test
  @DisplayName("findDead() honours the limit parameter")
  void findDead_respectsLimit() {
    Instant stale = Instant.now().minus(Duration.ofHours(1));
    for (int i = 0; i < 10; i++) {
      WorkerId id = new WorkerId("w-" + i);
      registry.register(workerInfo(id, "h-" + i));
      backdateHeartbeat(id, stale);
    }

    List<WorkerInfo> dead = registry.findDead(Duration.ofMinutes(1), 3);

    assertThat(dead).hasSize(3);
  }

  @Test
  @DisplayName("removeDead() deletes the given workers from the registry")
  void removeDead_deletesWorkers() {
    WorkerId keep = new WorkerId("keep");
    WorkerId drop1 = new WorkerId("drop-1");
    WorkerId drop2 = new WorkerId("drop-2");
    registry.register(workerInfo(keep, "h"));
    registry.register(workerInfo(drop1, "h"));
    registry.register(workerInfo(drop2, "h"));

    registry.removeDead(List.of(drop1, drop2));

    assertThat(registry.findById(keep)).isPresent();
    assertThat(registry.findById(drop1)).isEmpty();
    assertThat(registry.findById(drop2)).isEmpty();
  }

  @Test
  @DisplayName("removeDead() with an empty list is a no-op")
  void removeDead_emptyList_noOp() {
    registry.register(workerInfo(new WorkerId("w"), "h"));

    registry.removeDead(List.of());

    assertThat(registry.findAll()).hasSize(1);
  }

  @Test
  @DisplayName("deregister() removes a single worker")
  void deregister_removesWorker() {
    WorkerId id = new WorkerId("w");
    registry.register(workerInfo(id, "h"));

    registry.deregister(id);

    assertThat(registry.findById(id)).isEmpty();
  }

  @Test
  @DisplayName("deregister() on an unknown worker does not throw")
  void deregister_unknown_isNoThrow() {
    registry.deregister(new WorkerId("unknown"));
  }

  @Test
  @DisplayName("markGracefulStop() does not throw and leaves the worker findable")
  void markGracefulStop_leavesRecord() {
    WorkerId id = new WorkerId("w");
    registry.register(workerInfo(id, "h"));

    registry.markGracefulStop(id);

    assertThat(registry.findById(id)).isPresent();
  }

  protected WorkerInfo workerInfo(WorkerId id, String host) {
    return WorkerInfo.builder().id(id).host(host).metadata(Map.of()).build();
  }
}
