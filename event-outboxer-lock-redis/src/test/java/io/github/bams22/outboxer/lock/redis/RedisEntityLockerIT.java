/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.lock.redis;

import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.spi.contracts.AbstractEntityLockerContractTest;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

class RedisEntityLockerIT extends AbstractEntityLockerContractTest {

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

    @Override
    protected EntityLocker newLocker() {
        return new RedisEntityLocker(connection);
    }
}
