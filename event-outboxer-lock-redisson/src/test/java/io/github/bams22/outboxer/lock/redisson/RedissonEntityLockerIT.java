/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.lock.redisson;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.spi.EntityLocker.LockHandle;
import io.github.bams22.outboxer.spi.contracts.AbstractEntityLockerContractTest;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The Redisson locker against a real server, through the contract and with the Redisson-specific
 * shapes pinned: the watchdog is off, releases are thread-bound, a stale close after a takeover by
 * another thread is a no-op, and the native pub/sub wait beats polling.
 *
 * <p>The contract's TTL-expiry cases are opted out: they take over the expired key on the same
 * thread, which Redisson treats as re-entrance by the same owner — a shape the engine never
 * produces; the realistic one (successor on another thread) is tested below instead.
 */
class RedissonEntityLockerIT extends AbstractEntityLockerContractTest {

    @SuppressWarnings("resource")
    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    private static RedissonClient client;

    @BeforeAll
    static void boot() {
        REDIS.start();
        Config config = new Config();
        config.useSingleServer()
                .setAddress("redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
        client = Redisson.create(config);
    }

    @AfterAll
    static void shutdown() {
        if (client != null) {
            client.shutdown();
        }
        REDIS.stop();
    }

    @AfterEach
    void noKeyLeftBehind() {
        assertThat(client.getKeys().getKeysByPattern(RedissonEntityLocker.DEFAULT_KEY_PREFIX + "*"))
                .as("every test releases what it took")
                .isEmpty();
    }

    @Override
    protected EntityLocker newLocker() {
        return new RedissonEntityLocker(client);
    }

    @Test
    @DisplayName("keys live under the Redisson prefix, apart from the Lettuce locker's")
    void keysUseTheRedissonPrefix() {
        RedissonEntityLocker redisson = (RedissonEntityLocker) locker;
        assertThat(redisson.keyFor("order-42")).isEqualTo("outbox:rlock:order-42");
        try (LockHandle _ = locker.tryLock("order-42", Duration.ofSeconds(30)).orElseThrow()) {
            assertThat(client.getKeys().countExists("outbox:rlock:order-42")).isEqualTo(1);
            assertThat(client.getKeys().countExists("outbox:lock:order-42")).isZero();
        }
    }

    @Test
    @DisplayName("the watchdog is off: an unreleased lock expires at its TTL")
    void watchdogIsOff() throws Exception {
        LockHandle held = locker.tryLock("expiring", Duration.ofMillis(300)).orElseThrow();
        assertThat(client.getKeys().countExists("outbox:rlock:expiring")).isEqualTo(1);

        Thread.sleep(700); // well past the TTL; a watchdog would have renewed it by now

        assertThat(client.getKeys().countExists("outbox:rlock:expiring")).isZero();
        held.close(); // stale close: a debug-logged no-op
    }

    @Test
    @DisplayName("a stale close after another thread took the expired key is a no-op")
    void staleCloseDoesNotReleaseSuccessorOnAnotherThread() throws Exception {
        LockHandle zombie = locker.tryLock("contested", Duration.ofMillis(200)).orElseThrow();
        Thread.sleep(400); // the zombie's lease is gone

        AtomicReference<LockHandle> successor = new AtomicReference<>();
        CountDownLatch acquired = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread other =
                new Thread(
                        () -> {
                            successor.set(
                                    locker.tryLock("contested", Duration.ofSeconds(30))
                                            .orElseThrow());
                            acquired.countDown();
                            try {
                                release.await();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            successor.get().close();
                        },
                        "successor");
        other.start();
        assertThat(acquired.await(5, TimeUnit.SECONDS)).isTrue();

        zombie.close(); // must not touch the successor's lock

        assertThat(locker.tryLock("contested", Duration.ofSeconds(30)))
                .as("successor's lock must survive the zombie's stale close()")
                .isEmpty();
        release.countDown();
        other.join(TimeUnit.SECONDS.toMillis(5));
    }

    @Test
    @DisplayName("a handle closed on another thread releases the lock (unlock by owner thread id)")
    void closeFromAnotherThreadReleases() throws Exception {
        LockHandle held = locker.tryLock("elsewhere", Duration.ofSeconds(30)).orElseThrow();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread other =
                new Thread(
                        () -> {
                            try {
                                held.close();
                            } catch (Throwable t) {
                                failure.set(t);
                            }
                        },
                        "not-the-owner");
        other.start();
        other.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(failure.get()).isNull();
        assertThat(client.getKeys().countExists("outbox:rlock:elsewhere")).isZero();
        // The owner thread can take the key again: the guard entry is cleared on confirmation.
        assertThat(locker.tryLock("elsewhere", Duration.ofSeconds(30)))
                .isPresent()
                .get()
                .satisfies(LockHandle::close);
    }

    @Test
    @DisplayName(
            "the same thread cannot re-enter its own key: the SPI's busy, not Redisson's count")
    void noReentranceOnTheSameThread() {
        try (LockHandle _ = locker.tryLock("reentrant", Duration.ofSeconds(30)).orElseThrow()) {
            assertThat(locker.tryLock("reentrant", Duration.ofSeconds(30))).isEmpty();
            assertThat(locker.tryLock("reentrant", Duration.ofSeconds(30), Duration.ofMillis(50)))
                    .isEmpty();
        }
        assertThat(client.getKeys().countExists("outbox:rlock:reentrant")).isZero();
    }

    @Test
    @DisplayName("the native wait wakes on the holder's unlock, well inside a long budget")
    void nativeWaitWakesOnUnlock() throws Exception {
        // The holder releases on its acquiring thread (this one); the waiter runs elsewhere.
        LockHandle holder = locker.tryLock("wake", Duration.ofSeconds(30)).orElseThrow();
        Duration holdFor = Duration.ofMillis(100);
        CountDownLatch go = new CountDownLatch(1);
        AtomicReference<Optional<LockHandle>> result = new AtomicReference<>();
        Thread waiter =
                new Thread(
                        () -> {
                            go.countDown();
                            Optional<LockHandle> h =
                                    locker.tryLock(
                                            "wake", Duration.ofSeconds(30), Duration.ofSeconds(10));
                            h.ifPresent(LockHandle::close);
                            result.set(h);
                        },
                        "waiter");
        long start = System.nanoTime();
        waiter.start();
        assertThat(go.await(5, TimeUnit.SECONDS)).isTrue();
        sleepQuietly(holdFor);
        holder.close();
        waiter.join(TimeUnit.SECONDS.toMillis(10));
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        assertThat(waiter.isAlive()).isFalse();
        assertThat(result.get()).isPresent();
        assertThat(elapsed).isGreaterThanOrEqualTo(holdFor);
        assertThat(elapsed)
                .as("pub/sub wake-up, not the 10 s budget")
                .isLessThan(Duration.ofSeconds(3));
    }

    @Test
    @DisplayName("nativeWait=false polls through the SPI default and still acquires")
    void pollingVariant() throws Exception {
        RedissonEntityLocker polling =
                RedissonEntityLocker.builder().client(client).nativeWait(false).build();
        assertThat(polling.nativeWait()).isFalse();
        LockHandle holder = polling.tryLock("poll", Duration.ofSeconds(30)).orElseThrow();
        CountDownLatch go = new CountDownLatch(1);
        AtomicReference<Optional<LockHandle>> result = new AtomicReference<>();
        Thread waiter =
                new Thread(
                        () -> {
                            go.countDown();
                            Optional<LockHandle> h =
                                    polling.tryLock(
                                            "poll", Duration.ofSeconds(30), Duration.ofSeconds(5));
                            h.ifPresent(LockHandle::close);
                            result.set(h);
                        });
        waiter.start();
        assertThat(go.await(5, TimeUnit.SECONDS)).isTrue();
        sleepQuietly(Duration.ofMillis(60));
        holder.close();
        waiter.join(TimeUnit.SECONDS.toMillis(10));

        assertThat(result.get()).isPresent();
    }

    @Test
    @DisplayName("fair=true uses the fair lock and round-trips a lock")
    void fairLock() {
        RedissonEntityLocker fair =
                RedissonEntityLocker.builder().client(client).fair(true).build();
        assertThat(fair.fair()).isTrue();
        try (LockHandle _ = fair.tryLock("fair", Duration.ofSeconds(30)).orElseThrow()) {
            assertThat(fair.tryLock("fair", Duration.ofSeconds(30))).isEmpty();
        }
        assertThat(fair.tryLock("fair", Duration.ofSeconds(30)))
                .isPresent()
                .get()
                .satisfies(LockHandle::close);
    }

    @Test
    @DisplayName("a ttl below one millisecond is rejected")
    void ttlValidation() {
        assertThatThrownBy(() -> locker.tryLock("k", Duration.ofNanos(10)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ttl");
    }

    private static void sleepQuietly(Duration d) {
        try {
            Thread.sleep(d);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
