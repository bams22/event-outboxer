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
import static org.awaitility.Awaitility.await;

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.api.observer.EngineCrashedInfo;
import io.github.bams22.outboxer.core.engine.OutboxEngine;
import io.github.bams22.outboxer.core.polling.PollStrategy;
import io.github.bams22.outboxer.spring.health.OutboxHealthIndicator;
import io.github.bams22.outboxer.testkit.RecordingOutboxListener;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;

/**
 * Verifies end-to-end that a poller thread dying from an uncaught {@code Error} is detected by
 * the maintenance scheduler's health-check task, flips {@code OutboxEngine.state()} to STOPPED,
 * flips the engine-state metric to {@code state="stopped"=1}, and makes the Actuator health
 * indicator DOWN — so both k8s probes (via {@code outbox.health.probe-groups}) and Prometheus
 * alerts pick it up.
 *
 * <p>In its own test class because a crashed engine cannot be reused across further tests.
 */
@SpringBootTest(
    classes = InMemoryStarterCrashTest.TestApp.class,
    properties = {
      "event-outboxer.storage.type=inmemory",
      "event-outboxer.publisher.no-transaction-policy=IGNORE",
      "event-outboxer.event-types.defaults.poll-min-interval=20ms",
      "event-outboxer.event-types.defaults.poll-max-interval=50ms",
      "event-outboxer.event-types.defaults.handler-pool-size=1",
      "event-outboxer.maintenance.heartbeat-interval=100ms",
      "event-outboxer.maintenance.dead-threshold=1s",
      "event-outboxer.maintenance.orphan-recovery-interval=500ms",
      "event-outboxer.maintenance.watchdog-interval=100ms"
    })
class InMemoryStarterCrashTest {

  @Autowired OutboxEngine engine;
  @Autowired MeterRegistry meterRegistry;
  @Autowired OutboxHealthIndicator healthIndicator;
  @Autowired RecordingOutboxListener recording;

  @Test
  void pollerDeathFlipsStateMetricAndHealth() {
    // Startup is synchronous — engine is RUNNING when the context is ready; first poll tick
    // triggers the injected strategy's Error.
    await()
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(50))
        .until(() -> engine.state() == OutboxEngine.State.STOPPED);

    // Metric flipped to stopped=1:
    assertThat(
            meterRegistry.get("event_outboxer.engine.state").tag("state", "stopped").gauge().value())
        .isEqualTo(1.0);
    assertThat(
            meterRegistry.get("event_outboxer.engine.state").tag("state", "running").gauge().value())
        .isEqualTo(0.0);

    // Counter for the crash event fired once:
    assertThat(meterRegistry.get("event_outboxer.engine.crashed").counter().count())
        .isGreaterThanOrEqualTo(1.0);

    // Health indicator DOWN, which (with outbox.health.probe-groups: [readiness]) would
    // propagate into the Actuator readiness probe.
    assertThat(healthIndicator.health().getStatus()).isEqualTo(Status.DOWN);

    // Lifecycle is still active — Spring will call stop() on context close, cleanup runs
    // normally (worker deregister, handler drain).
    assertThat(engine.isLifecycleActive()).isTrue();

    // onEngineCrashed listener fired exactly once with a non-blank reason. The metric counter
    // is derived from this callback, so covering both keeps the wire trustable end-to-end.
    assertThat(recording.engineCrashed()).hasSize(1);
    EngineCrashedInfo info = recording.engineCrashed().get(0);
    assertThat(info.reason()).isNotBlank();
    assertThat(info.workerId()).isNotNull();
  }

  static class NoopHandler implements EventHandler<String> {
    @Override
    public String eventType() {
      return "NOOP";
    }

    @Override
    public Class<String> payloadType() {
      return String.class;
    }

    @Override
    public EventOutcome handle(EventContext ctx, String payload) {
      return EventOutcome.Success.INSTANCE;
    }
  }

  @SpringBootConfiguration
  @EnableAutoConfiguration(
      exclude = {
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        JdbcTemplateAutoConfiguration.class
      })
  static class TestApp {

    @Bean
    NoopHandler noopHandler() {
      return new NoopHandler();
    }

    /**
     * Strategy that dies on first claim attempt — triggers poller thread death.
     *
     * <p>Plain {@link Error} on purpose: {@code VirtualMachineError} subclasses like
     * {@code OutOfMemoryError} are interpreted by some JVM tooling (surefire fork, ByteBuddy
     * agent, certain JDK 25 builds) as fatal for the whole JVM, even when thrown synthetically.
     * Plain {@code Error} keeps the crash-detection semantics (non-{@code RuntimeException},
     * kills the thread) without triggering JVM-critical handling.
     */
    @Bean
    PollStrategy crashingPollStrategy() {
      return (eventType, workerId, batchSize) -> {
        throw new Error("simulated crash in InMemoryStarterCrashTest");
      };
    }

    @Bean
    MeterRegistry meterRegistry() {
      return new SimpleMeterRegistry();
    }

    @Bean
    RecordingOutboxListener recordingListener() {
      return new RecordingOutboxListener();
    }
  }
}
