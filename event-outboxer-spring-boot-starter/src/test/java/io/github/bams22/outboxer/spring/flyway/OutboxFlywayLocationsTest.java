/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.flyway;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.lock.postgres.lease.PgLeaseEntityLocker;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.FilteredClassLoader;

class OutboxFlywayLocationsTest {

    @Test
    @DisplayName("core and archive always apply; lock joins when the lease module is present")
    void leaseModulePresent() {
        assertThat(OutboxFlywayLocations.resolve(getClass().getClassLoader()))
                .containsExactly(
                        OutboxFlywayLocations.CORE,
                        OutboxFlywayLocations.ARCHIVE,
                        OutboxFlywayLocations.LOCK);
    }

    @Test
    @DisplayName("without the lease module the lock lane is left out")
    void leaseModuleAbsent() {
        ClassLoader withoutLease = new FilteredClassLoader(PgLeaseEntityLocker.class);
        assertThat(OutboxFlywayLocations.resolve(withoutLease))
                .containsExactly(OutboxFlywayLocations.CORE, OutboxFlywayLocations.ARCHIVE);
    }

    @Test
    @DisplayName(
            "the locations live outside db/migration so an application instance never sees them")
    void outsideApplicationTree() {
        assertThat(OutboxFlywayLocations.resolve(null))
                .allSatisfy(
                        location ->
                                assertThat(location)
                                        .startsWith("classpath:event-outboxer/migration/")
                                        .doesNotContain("db/migration"));
    }
}
