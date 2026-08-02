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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ADR-0012: the PG advisory locker consumes one pooled connection per held lock, so a handler fleet
 * at least as large as the pool can deadlock against itself — the startup warning must fire exactly
 * on that boundary.
 */
class PgLockPoolWarningTest {

    @Test
    @DisplayName("warns when total handler threads reach the pool size")
    void warnsAtBoundary() {
        assertThat(OutboxEngineAutoConfiguration.pgLockPoolWarning(10, 10))
                .isPresent()
                .get()
                .asString()
                .contains("maximum-pool-size")
                // Since ADR-0022 the self-deadlock applies to the advisory opt-out only; the
                // message must name the mode and point at the lease locker as the fix.
                .contains("postgres-advisory")
                .contains("lock.type=postgres-lease,");
        assertThat(OutboxEngineAutoConfiguration.pgLockPoolWarning(23, 10)).isPresent();
    }

    @Test
    @DisplayName("silent while the pool is strictly larger than the handler fleet")
    void silentBelowBoundary() {
        assertThat(OutboxEngineAutoConfiguration.pgLockPoolWarning(9, 10)).isEmpty();
    }
}
