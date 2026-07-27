/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.cache.redis;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.spi.MetricsSnapshotCache;
import io.github.bams22.outboxer.spi.OutboxMetricsSnapshot;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

class LettuceMetricsSnapshotCacheIT {

  @SuppressWarnings("resource")
  private static final GenericContainer<?> REDIS =
      new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

  private static RedisClient client;
  private static StatefulRedisConnection<String, String> connection;

  @BeforeAll
  static void boot() {
    REDIS.start();
    client =
        RedisClient.create(
            RedisURI.builder()
                .withHost(REDIS.getHost())
                .withPort(REDIS.getMappedPort(6379))
                .build());
    connection = client.connect();
  }

  @AfterAll
  static void shutdown() {
    if (connection != null) {
      connection.close();
    }
    if (client != null) {
      client.shutdown();
    }
    REDIS.stop();
  }

  @BeforeEach
  void flushBetweenTests() {
    connection.sync().flushdb();
  }

  private static final OutboxMetricsSnapshot SNAPSHOT =
      OutboxMetricsSnapshot.builder()
          .totalPending(7)
          .totalProcessing(3)
          .totalDisabled(1)
          .oldestPendingRunAt(Instant.parse("2026-04-22T12:00:00Z"))
          .oldestClaimedAt(Instant.parse("2026-04-22T12:01:00Z"))
          .takenAt(Instant.parse("2026-04-22T12:02:00Z"))
          .perType(
              List.of(
                  OutboxMetricsSnapshot.EventTypeStats.builder()
                      .eventType("ORDER")
                      .pending(5)
                      .processing(2)
                      .disabled(0)
                      .oldestPendingRunAt(Instant.parse("2026-04-22T12:00:00Z"))
                      .build()))
          .build();

  @Test
  void putThenGetReturnsDeserialisedSnapshot() {
    MetricsSnapshotCache cache =
        new LettuceMetricsSnapshotCache(connection, Duration.ofSeconds(30));

    cache.put(SNAPSHOT);

    assertThat(cache.get()).contains(SNAPSHOT);
  }

  @Test
  void getReturnsEmptyWhenKeyAbsent() {
    MetricsSnapshotCache cache =
        new LettuceMetricsSnapshotCache(connection, Duration.ofSeconds(30));

    assertThat(cache.get()).isEmpty();
  }

  @Test
  void invalidateRemovesEntry() {
    MetricsSnapshotCache cache =
        new LettuceMetricsSnapshotCache(connection, Duration.ofSeconds(30));
    cache.put(SNAPSHOT);

    cache.invalidate();

    assertThat(cache.get()).isEmpty();
  }

  @Test
  void serverSideTtlExpiresTheEntry() throws InterruptedException {
    MetricsSnapshotCache cache =
        new LettuceMetricsSnapshotCache(connection, Duration.ofMillis(200));
    cache.put(SNAPSHOT);
    assertThat(cache.get()).as("entry present immediately after put").contains(SNAPSHOT);

    Thread.sleep(400);

    assertThat(cache.get()).as("entry gone after TTL").isEmpty();
  }

  @Test
  void customKeyPrefixIsHonoured() {
    MetricsSnapshotCache a =
        new LettuceMetricsSnapshotCache(
            connection,
            Duration.ofSeconds(30),
            "tenant-a:outbox:metrics:",
            new com.fasterxml.jackson.databind.json.JsonMapper().findAndRegisterModules());
    MetricsSnapshotCache b =
        new LettuceMetricsSnapshotCache(
            connection,
            Duration.ofSeconds(30),
            "tenant-b:outbox:metrics:",
            new com.fasterxml.jackson.databind.json.JsonMapper().findAndRegisterModules());

    a.put(SNAPSHOT);

    assertThat(a.get()).contains(SNAPSHOT);
    assertThat(b.get()).isEmpty();
  }
}
