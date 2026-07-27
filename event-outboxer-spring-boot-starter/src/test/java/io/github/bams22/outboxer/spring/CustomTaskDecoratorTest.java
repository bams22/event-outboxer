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
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spring.storage.OutboxInMemoryTestConfiguration;
import java.time.Duration;
import java.util.UUID;
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
import org.springframework.context.annotation.Import;
import org.springframework.core.task.TaskDecorator;

/**
 * Verifies that a user-defined {@code @Bean TaskDecorator} is applied to the handler executor
 * instead of the default {@code ContextPropagatingTaskDecorator} — the wiring in {@code
 * OutboxEngineAutoConfiguration}.
 */
@SpringBootTest(
    classes = CustomTaskDecoratorTest.TestApp.class,
    properties = {
      "event-outboxer.publisher.no-transaction-policy=IGNORE",
      "event-outboxer.event-types.defaults.poll-min-interval=20ms",
      "event-outboxer.event-types.defaults.poll-max-interval=50ms",
      "event-outboxer.event-types.defaults.handler-pool-size=1",
      "event-outboxer.maintenance.heartbeat-interval=200ms",
      "event-outboxer.maintenance.dead-threshold=1s",
      "event-outboxer.maintenance.orphan-recovery-interval=500ms",
      "event-outboxer.maintenance.watchdog-interval=500ms"
    })
@Import(OutboxInMemoryTestConfiguration.class)
class CustomTaskDecoratorTest {

  @Autowired OutboxEventPublisher publisher;
  @Autowired EventStore store;
  @Autowired CountingTaskDecorator decorator;
  @Autowired RecordingHandler handler;

  @Test
  void userDefinedTaskDecoratorIsAppliedToHandlerExecutor() {
    UUID id = publisher.publish("ORDER", new OrderCreated("ord-1"));

    await()
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(20))
        .until(() -> store.findById(id).isEmpty());

    assertThat(handler.invocationCount()).isGreaterThanOrEqualTo(1);
    assertThat(decorator.decorations())
        .as("custom TaskDecorator must wrap at least one handler submission")
        .isGreaterThanOrEqualTo(1);
  }

  record OrderCreated(String orderId) {}

  static class RecordingHandler implements EventHandler<OrderCreated> {
    private final AtomicInteger invocations = new AtomicInteger();

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
      return EventOutcome.Success.INSTANCE;
    }

    int invocationCount() {
      return invocations.get();
    }
  }

  static final class CountingTaskDecorator implements TaskDecorator {
    private final AtomicInteger count = new AtomicInteger();

    @Override
    public Runnable decorate(Runnable runnable) {
      count.incrementAndGet();
      return runnable;
    }

    int decorations() {
      return count.get();
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
    CountingTaskDecorator customTaskDecorator() {
      return new CountingTaskDecorator();
    }
  }
}
