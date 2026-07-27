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
import org.junit.jupiter.api.Assumptions;
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

  /**
   * Opt-in hook for TTL-honouring lockers (ADR-0022). Return {@code true} and implement {@link
   * #forceExpire(String)} to activate the TTL-expiry contract tests; the default keeps them
   * skipped, which preserves the historic contract for lockers whose TTL is best-effort or ignored
   * (PG advisory) and for backends where expiry cannot be forced deterministically.
   */
  protected boolean supportsTtlExpiry() {
    return false;
  }

  /**
   * Force the lease/lock of {@code key} into the expired state, as if its TTL had elapsed — without
   * waiting wall-clock time. Only called when {@link #supportsTtlExpiry()} is {@code true}.
   * Implementations that track both an acquisition and an expiry timestamp must backdate both (the
   * lease table's CHECK requires {@code expires_at > acquired_at}).
   */
  protected void forceExpire(String key) {
    throw new UnsupportedOperationException(
        "forceExpire() must be implemented when supportsTtlExpiry() returns true");
  }

  @BeforeEach
  void setUpLocker() {
    locker = newLocker();
  }

  @Test
  @DisplayName("tryLock() returns a handle when the key is free")
  void tryLock_free_returnsHandle() {
    try (LockHandle handle = locker.tryLock("key-1", Duration.ofSeconds(30)).orElseThrow()) {
      assertThat(handle).isNotNull();
    }
  }

  @Test
  @DisplayName("tryLock() returns empty when another holder still owns the same key")
  void tryLock_busy_returnsEmpty() {
    try (LockHandle _ = locker.tryLock("busy", Duration.ofSeconds(30)).orElseThrow()) {
      Optional<LockHandle> second = locker.tryLock("busy", Duration.ofSeconds(30));
      assertThat(second).isEmpty();
    }
  }

  @Test
  @DisplayName("tryLock() on a different key succeeds while another key is held")
  void tryLock_independentKeys() {
    try (LockHandle _ = locker.tryLock("a", Duration.ofSeconds(30)).orElseThrow()) {
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
  @DisplayName("an expired lock is available to the next acquirer (TTL-honouring lockers only)")
  void ttlExpiry_allowsTakeover() {
    Assumptions.assumeTrue(supportsTtlExpiry(), "locker does not support deterministic expiry");
    LockHandle dead = locker.tryLock("expiring", Duration.ofSeconds(30)).orElseThrow();
    forceExpire("expiring");

    Optional<LockHandle> successor = locker.tryLock("expiring", Duration.ofSeconds(30));
    assertThat(successor).isPresent();
    successor.orElseThrow().close();
    dead.close();
  }

  @Test
  @DisplayName("a stale close() must not release the successor's lock (TTL-honouring lockers only)")
  void ttlExpiry_staleCloseDoesNotReleaseSuccessor() {
    Assumptions.assumeTrue(supportsTtlExpiry(), "locker does not support deterministic expiry");
    LockHandle zombie = locker.tryLock("contested", Duration.ofSeconds(30)).orElseThrow();
    forceExpire("contested");
    LockHandle successor = locker.tryLock("contested", Duration.ofSeconds(30)).orElseThrow();

    // The zombie's token no longer matches the row/key — its close() must be a silent no-op.
    zombie.close();

    Optional<LockHandle> third = locker.tryLock("contested", Duration.ofSeconds(30));
    assertThat(third).as("successor's lock must survive the zombie's stale close()").isEmpty();
    successor.close();
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
