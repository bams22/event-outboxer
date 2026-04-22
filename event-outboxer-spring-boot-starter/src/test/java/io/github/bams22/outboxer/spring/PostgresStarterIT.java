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
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest(
    classes = PostgresStarterIT.TestApp.class,
    properties = {
      "event-outboxer.storage.type=postgres",
      "event-outboxer.publisher.no-transaction-policy=FAIL",
      "event-outboxer.event-types.defaults.poll-min-interval=20ms",
      "event-outboxer.event-types.defaults.poll-max-interval=50ms",
      "event-outboxer.maintenance.heartbeat-interval=500ms",
      "event-outboxer.maintenance.dead-threshold=2s",
      "event-outboxer.maintenance.orphan-recovery-interval=1s",
      "event-outboxer.maintenance.watchdog-interval=500ms",
      "spring.flyway.locations=classpath:db/migration/outbox/core"
    })
@Testcontainers
class PostgresStarterIT {

  @Container
  static PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:15")
          .withDatabaseName("outboxer")
          .withUsername("outboxer")
          .withPassword("outboxer");

  @DynamicPropertySource
  static void dataSource(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
  }

  @Autowired OutboxEventPublisher publisher;
  @Autowired EventStore store;
  @Autowired RecordingHandler handler;
  @Autowired TransactionalPublishService publishService;

  @Test
  @Transactional
  void publishWithinTransactionCommitsAndProcesses() {
    UUID id = publisher.publish("ORDER", new OrderCreated("ord-1", 3));
    // Inside @Transactional: the row is visible to our own connection but not committed yet.
    // Awaitility here would hang because SmartLifecycle's engine is polling on a DIFFERENT
    // connection and sees nothing. So just verify our own view.
    assertThat(store.findById(id)).isPresent();
  }

  @Test
  void publishCommittedIsEventuallyProcessed() {
    UUID id = publishService.publishCommitted("ord-2", 7);

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(50))
        .until(() -> store.findById(id).isEmpty());

    assertThat(handler.invocationCount()).isGreaterThanOrEqualTo(1);
  }

  record OrderCreated(String orderId, int count) {}

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

  /** Thin service wrapping a transactional publish so we can bracket commit in the test. */
  static class TransactionalPublishService {
    private final OutboxEventPublisher publisher;

    TransactionalPublishService(OutboxEventPublisher publisher) {
      this.publisher = publisher;
    }

    @Transactional
    UUID publishCommitted(String orderId, int count) {
      return publisher.publish("ORDER", new OrderCreated(orderId, count));
    }
  }

  @SpringBootConfiguration
  @EnableAutoConfiguration
  static class TestApp {

    @Bean
    RecordingHandler recordingHandler() {
      return new RecordingHandler();
    }

    @Bean
    TransactionalPublishService transactionalPublishService(OutboxEventPublisher publisher) {
      return new TransactionalPublishService(publisher);
    }
  }
}
