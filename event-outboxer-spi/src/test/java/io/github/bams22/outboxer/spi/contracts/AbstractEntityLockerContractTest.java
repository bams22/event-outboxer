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

import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.spi.EntityLocker.LockHandle;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Reusable contract specification for every {@link EntityLocker} implementation. Focuses on the
 * semantics the dispatcher relies on: {@link EntityLocker#tryLock(String, Duration)} is
 * non-blocking, busy locks return {@link Optional#empty()}, and a released handle makes the lock
 * available again.
 */
public abstract class AbstractEntityLockerContractTest {

  protected EntityLocker locker;

  protected abstract EntityLocker newLocker();

  @BeforeEach
  void setUpLocker() {
    locker = newLocker();
  }

  @Test
  @DisplayName("tryLock() returns a handle when the key is free")
  void tryLock_free_returnsHandle() {
    try (LockHandle handle =
        locker.tryLock("key-1", Duration.ofSeconds(30)).orElseThrow()) {
      assertThat(handle).isNotNull();
    }
  }

  @Test
  @DisplayName("tryLock() returns empty when another holder still owns the same key")
  void tryLock_busy_returnsEmpty() {
    try (LockHandle ignored = locker.tryLock("busy", Duration.ofSeconds(30)).orElseThrow()) {
      Optional<LockHandle> second = locker.tryLock("busy", Duration.ofSeconds(30));
      assertThat(second).isEmpty();
    }
  }

  @Test
  @DisplayName("tryLock() on a different key succeeds while another key is held")
  void tryLock_independentKeys() {
    try (LockHandle ignored = locker.tryLock("a", Duration.ofSeconds(30)).orElseThrow()) {
      Optional<LockHandle> b = locker.tryLock("b", Duration.ofSeconds(30));
      assertThat(b).isPresent();
      b.orElseThrow().close();
    }
  }

  @Test
  @DisplayName("release via close() makes the key available to a subsequent acquirer")
  void close_releasesLock() {
    LockHandle first = locker.tryLock("key-cycle", Duration.ofSeconds(30)).orElseThrow();
    first.close();

    Optional<LockHandle> second = locker.tryLock("key-cycle", Duration.ofSeconds(30));
    assertThat(second).isPresent();
    second.orElseThrow().close();
  }

  @Test
  @DisplayName("close() is idempotent — releasing twice must not throw")
  void close_idempotent() {
    LockHandle handle = locker.tryLock("key-twice", Duration.ofSeconds(30)).orElseThrow();
    handle.close();
    handle.close();
  }

  @Test
  @DisplayName("tryLock() from concurrent threads grants the lock to exactly one holder per key")
  void tryLock_concurrent_exclusivity() throws Exception {
    int threads = 32;
    String key = "hot-key";
    ExecutorService exec = Executors.newFixedThreadPool(threads);
    try {
      CountDownLatch go = new CountDownLatch(1);
      AtomicInteger acquired = new AtomicInteger();
      ConcurrentHashMap.KeySetView<LockHandle, Boolean> handles = ConcurrentHashMap.newKeySet();
      var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
      for (int i = 0; i < threads; i++) {
        futures.add(
            exec.submit(
                () -> {
                  try {
                    go.await();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                  }
                  Optional<LockHandle> h = locker.tryLock(key, Duration.ofSeconds(30));
                  h.ifPresent(
                      handle -> {
                        acquired.incrementAndGet();
                        handles.add(handle);
                      });
                }));
      }
      go.countDown();
      for (var f : futures) {
        f.get(10, TimeUnit.SECONDS);
      }
      assertThat(acquired).hasValue(1);
      handles.forEach(LockHandle::close);
    } finally {
      exec.shutdownNow();
    }
  }
}
