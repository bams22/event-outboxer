/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.lock.postgres.lease;

import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bams22.outboxer.domain.exception.LockAcquisitionException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import javax.sql.DataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Argument validation happens before any connection is borrowed (ADR-0022 §JDBC contract, items
 * 4-5) — verified with a DataSource that fails the test on first touch.
 */
class PgLeaseEntityLockerValidationTest {

    private final PgLeaseEntityLocker locker = new PgLeaseEntityLocker(untouchableDataSource());

    @Test
    @DisplayName("null key / null ttl are rejected before touching the pool")
    void nullArguments() {
        assertThatNullPointerException()
                .isThrownBy(() -> locker.tryLock(null, Duration.ofMinutes(1)));
        assertThatNullPointerException().isThrownBy(() -> locker.tryLock("k", null));
    }

    @Test
    @DisplayName("sub-millisecond, zero and negative TTLs are rejected with a clear message")
    void ttlFloor() {
        for (Duration bad :
                new Duration[] {Duration.ofNanos(999_999), Duration.ZERO, Duration.ofSeconds(-1)}) {
            assertThatThrownBy(() -> locker.tryLock("k", bad))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("1ms");
        }
    }

    @Test
    @DisplayName("keys longer than VARCHAR(512) are rejected with a clear message")
    void keyLength() {
        String overlong = "x".repeat(PgLeaseEntityLocker.MAX_KEY_LENGTH + 1);
        assertThatThrownBy(() -> locker.tryLock(overlong, Duration.ofMinutes(1)))
                .isInstanceOf(LockAcquisitionException.class)
                .hasMessageContaining("512")
                .hasMessageContaining("extractLockKey");
    }

    @Test
    @DisplayName("a 512-character key passes validation (and only then touches the pool)")
    void keyLengthBoundaryReachesDataSource() {
        String maxKey = "x".repeat(PgLeaseEntityLocker.MAX_KEY_LENGTH);
        assertThatThrownBy(() -> locker.tryLock(maxKey, Duration.ofMinutes(1)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("DataSource must not be touched");
    }

    private static DataSource untouchableDataSource() {
        return (DataSource)
                Proxy.newProxyInstance(
                        PgLeaseEntityLockerValidationTest.class.getClassLoader(),
                        new Class<?>[] {DataSource.class},
                        (proxy, method, args) -> {
                            throw new AssertionError(
                                    "DataSource must not be touched: " + method.getName());
                        });
    }
}
