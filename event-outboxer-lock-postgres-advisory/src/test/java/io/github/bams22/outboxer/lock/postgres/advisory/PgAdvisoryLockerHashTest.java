/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.lock.postgres.advisory;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PgAdvisoryLockerHashTest {

    @Test
    void hashIsDeterministic() {
        assertThat(PgAdvisoryLocker.hash("order:42")).isEqualTo(PgAdvisoryLocker.hash("order:42"));
    }

    @Test
    void hashDiffersForDifferentKeys() {
        assertThat(PgAdvisoryLocker.hash("order:42"))
                .isNotEqualTo(PgAdvisoryLocker.hash("order:43"));
    }

    @Test
    void hashStableAcrossCalls() {
        long first = PgAdvisoryLocker.hash("aggregate:abc");
        long second = PgAdvisoryLocker.hash("aggregate:abc");
        long third = PgAdvisoryLocker.hash("aggregate:abc");
        assertThat(first).isEqualTo(second).isEqualTo(third);
    }
}
