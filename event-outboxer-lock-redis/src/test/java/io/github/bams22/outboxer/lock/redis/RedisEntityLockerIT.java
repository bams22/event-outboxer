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

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.spi.EntityLocker.LockHandle;
import io.github.bams22.outboxer.spi.contracts.AbstractEntityLockerContractTest;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The Redis locker against a real server, through the contract and with the pub/sub wake-up wired
 * (ADR-0035): the contract's bounded-wait cases therefore exercise the notification path, and the
 * tests below pin what distinguishes it from polling.
 */
class RedisEntityLockerIT extends AbstractEntityLockerContractTest {

    @SuppressWarnings("resource")
    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    private static RedisClient client;
    private static StatefulRedisConnection<String, String> connection;
    private static StatefulRedisPubSubConnection<String, String> pubSub;

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
        pubSub = client.connectPubSub();
    }

    @AfterAll
    static void shutdown() {
        if (pubSub != null) {
            pubSub.close();
        }
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

    @AfterEach
    void noSubscriptionLeftBehind() {
        // Every wait unsubscribes on the way out: the server must hold no lock channel.
        assertThat(connection.sync().pubsubChannels(RedisEntityLocker.DEFAULT_KEY_PREFIX + "*"))
                .isEmpty();
        if (locker instanceof RedisEntityLocker redis) {
            assertThat(redis.waitingKeys()).isZero();
        }
    }

    @Override
    protected EntityLocker newLocker() {
        return RedisEntityLocker.builder().connection(connection).wakeupConnection(pubSub).build();
    }

    @Test
    @DisplayName("a waiter wakes on the release notification, not on the fallback probe")
    void wakeup_beatsTheFallbackProbe() throws Exception {
        // A fallback of 5 s: if the waiter acquires well before that, the notification did it.
        RedisEntityLocker slowFallback =
                RedisEntityLocker.builder()
                        .connection(connection)
                        .wakeupConnection(pubSub)
                        .fallbackProbeInterval(Duration.ofSeconds(5))
                        .build();
        assertThat(slowFallback.wakeupEnabled()).isTrue();
        LockHandle holder = slowFallback.tryLock("wake", Duration.ofSeconds(30)).orElseThrow();
        Duration holdFor = Duration.ofMillis(100);
        Thread releaser =
                new Thread(
                        () -> {
                            sleepQuietly(holdFor);
                            holder.close();
                        },
                        "releaser");
        long start = System.nanoTime();
        releaser.start();

        Optional<LockHandle> waiter =
                slowFallback.tryLock("wake", Duration.ofSeconds(30), Duration.ofSeconds(10));
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
        releaser.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(waiter).isPresent();
        assertThat(elapsed).isGreaterThanOrEqualTo(holdFor);
        assertThat(elapsed)
                .as("woken by PUBLISH, not by the 5 s fallback")
                .isLessThan(Duration.ofSeconds(2));
        waiter.orElseThrow().close();
        assertThat(slowFallback.waitingKeys()).isZero();
    }

    @Test
    @DisplayName("a crowd of waiters on one key all get their turn, one at a time")
    void wakeup_manyWaitersTakeTurns() throws Exception {
        int waiters = 16;
        String key = "crowd";
        LockHandle holder = locker.tryLock(key, Duration.ofSeconds(30)).orElseThrow();
        CountDownLatch ready = new CountDownLatch(waiters);
        AtomicInteger acquired = new AtomicInteger();
        AtomicInteger overlaps = new AtomicInteger();
        AtomicInteger inside = new AtomicInteger();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < waiters; i++) {
            Thread t =
                    new Thread(
                            () -> {
                                ready.countDown();
                                Optional<LockHandle> h =
                                        locker.tryLock(
                                                key,
                                                Duration.ofSeconds(30),
                                                Duration.ofSeconds(20));
                                if (h.isEmpty()) {
                                    return;
                                }
                                if (inside.incrementAndGet() > 1) {
                                    overlaps.incrementAndGet();
                                }
                                sleepQuietly(Duration.ofMillis(5));
                                inside.decrementAndGet();
                                acquired.incrementAndGet();
                                h.get().close();
                            },
                            "waiter-" + i);
            threads.add(t);
            t.start();
        }
        assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(100);
        assertThat(((RedisEntityLocker) locker).waitingKeys()).isEqualTo(1);

        holder.close();
        for (Thread t : threads) {
            t.join(TimeUnit.SECONDS.toMillis(30));
            assertThat(t.isAlive()).isFalse();
        }

        assertThat(acquired).hasValue(waiters);
        assertThat(overlaps).hasValue(0);
    }

    @Test
    @DisplayName("without a pub/sub connection the bounded wait still works by polling")
    void polling_withoutWakeupConnection() throws Exception {
        RedisEntityLocker polling = new RedisEntityLocker(connection);
        assertThat(polling.wakeupEnabled()).isFalse();
        LockHandle holder = polling.tryLock("poll", Duration.ofSeconds(30)).orElseThrow();
        Thread releaser =
                new Thread(
                        () -> {
                            sleepQuietly(Duration.ofMillis(60));
                            holder.close();
                        });
        releaser.start();

        Optional<LockHandle> waiter =
                polling.tryLock("poll", Duration.ofSeconds(30), Duration.ofSeconds(5));
        releaser.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(waiter).isPresent();
        waiter.orElseThrow().close();
    }

    @Test
    @DisplayName("the release script publishes on the key's channel")
    void release_publishesOnTheKeyChannel() throws Exception {
        RedisEntityLocker redis = (RedisEntityLocker) locker;
        String channel = redis.channelFor("pub");
        assertThat(channel).isEqualTo("outbox:lock:released:pub");
        try (StatefulRedisPubSubConnection<String, String> probe = client.connectPubSub()) {
            CountDownLatch received = new CountDownLatch(1);
            probe.addListener(
                    new io.lettuce.core.pubsub.RedisPubSubAdapter<String, String>() {
                        @Override
                        public void message(String ch, String message) {
                            if (channel.equals(ch)) {
                                received.countDown();
                            }
                        }
                    });
            probe.sync().subscribe(channel);

            locker.tryLock("pub", Duration.ofSeconds(30)).orElseThrow().close();

            assertThat(received.await(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static void sleepQuietly(Duration d) {
        try {
            Thread.sleep(d);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
