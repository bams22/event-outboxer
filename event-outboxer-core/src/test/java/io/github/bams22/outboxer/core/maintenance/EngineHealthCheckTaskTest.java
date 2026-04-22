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

import io.github.bams22.outboxer.core.polling.Poller;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class EngineHealthCheckTaskTest {

  @Test
  void noReport_whenAllPollersHealthy() {
    Poller healthy = makePoller("HEALTHY", false);
    AtomicInteger reports = new AtomicInteger();

    new EngineHealthCheckTask(List.of(healthy), (reason, cause) -> reports.incrementAndGet()).run();

    assertThat(reports).hasValue(0);
  }

  @Test
  void reportsOnce_whenOnePollerCrashed() {
    Poller healthy = makePoller("HEALTHY", false);
    Poller dead = makePoller("DEAD", true);
    AtomicInteger reports = new AtomicInteger();
    StringBuilder reasons = new StringBuilder();

    new EngineHealthCheckTask(
            List.of(healthy, dead),
            (reason, cause) -> {
              reports.incrementAndGet();
              reasons.append(reason);
            })
        .run();

    assertThat(reports).hasValue(1);
    assertThat(reasons.toString()).contains("DEAD");
  }

  @Test
  void stopsAtFirstCrash_noDoubleReport() {
    Poller dead1 = makePoller("D1", true);
    Poller dead2 = makePoller("D2", true);
    AtomicInteger reports = new AtomicInteger();

    new EngineHealthCheckTask(
            List.of(dead1, dead2), (reason, cause) -> reports.incrementAndGet())
        .run();

    assertThat(reports).hasValue(1);
  }

  private static Poller makePoller(String eventType, boolean crashed) {
    Poller mock = Mockito.mock(Poller.class);
    Mockito.when(mock.eventType()).thenReturn(eventType);
    Mockito.when(mock.isCrashed()).thenReturn(crashed);
    return mock;
  }
}
