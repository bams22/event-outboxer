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
import io.github.bams22.outboxer.api.publish.OutboxEventPublisher;
import io.github.bams22.outboxer.core.engine.OutboxEngine;
import io.github.bams22.outboxer.spi.EventStore;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;

@SpringBootTest(
    classes = InMemoryStarterSmokeTest.TestApp.class,
    properties = {
      "outbox.storage.type=inmemory",
      "outbox.publisher.no-transaction-policy=IGNORE",
      "outbox.event-types.defaults.poll-min-interval=20ms",
      "outbox.event-types.defaults.poll-max-interval=50ms",
      "outbox.event-types.defaults.handler-pool-size=2",
      "outbox.maintenance.heartbeat-interval=200ms",
      "outbox.maintenance.dead-threshold=1s",
      "outbox.maintenance.orphan-recovery-interval=500ms",
      "outbox.maintenance.watchdog-interval=500ms"
    })
class InMemoryStarterSmokeTest {

  @Autowired OutboxEventPublisher publisher;
  @Autowired EventStore store;
  @Autowired OutboxEngine engine;
  @Autowired RecordingHandler handler;
  @Autowired MeterRegistry meterRegistry;

  @Test
  void publishedEventIsProcessed() {
    assertThat(engine.state()).isEqualTo(OutboxEngine.State.RUNNING);

    UUID id = publisher.publish("ORDER", new OrderCreated("ord-1", 3));

    await()
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(20))
        .until(() -> store.findById(id).isEmpty());

    assertThat(handler.invocationCount()).isGreaterThanOrEqualTo(1);
    assertThat(handler.seen()).containsKey("ord-1");
  }

  @Test
  void engineStateGaugesReflectRunningState() {
    assertThat(engine.state()).isEqualTo(OutboxEngine.State.RUNNING);

    assertThat(
            meterRegistry.get("event_outboxer.engine.state").tag("state", "running").gauge().value())
        .isEqualTo(1.0);
    assertThat(
            meterRegistry.get("event_outboxer.engine.state").tag("state", "stopped").gauge().value())
        .isEqualTo(0.0);
    assertThat(
            meterRegistry.get("event_outboxer.engine.state").tag("state", "stopping").gauge().value())
        .isEqualTo(0.0);
  }

  record OrderCreated(String orderId, int count) {}

  static class RecordingHandler implements EventHandler<OrderCreated> {
    private final AtomicInteger invocations = new AtomicInteger();
    private final ConcurrentHashMap<String, Integer> seen = new ConcurrentHashMap<>();

    @Override
    public String eventType() {
      return "ORDER";
    }

    @Override
    public Class<OrderCreated> payloadType() {
      return OrderCreated.class;
    }

    @Override
    public EventOutcome handle(EventContext ctx, OrderCreated payload) {
      invocations.incrementAndGet();
      seen.put(payload.orderId(), payload.count());
      return EventOutcome.Success.INSTANCE;
    }

    int invocationCount() {
      return invocations.get();
    }

    ConcurrentHashMap<String, Integer> seen() {
      return seen;
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
    RecordingHandler recordingHandler() {
      return new RecordingHandler();
    }

    @Bean
    MeterRegistry meterRegistry() {
      return new SimpleMeterRegistry();
    }
  }
}
