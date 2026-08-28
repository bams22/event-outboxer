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

import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.Nullable;
import org.springframework.util.ClassUtils;

/**
 * Classpath locations of the migrations the library ships, and which of them the starter-managed
 * Flyway instance applies (ADR-0028).
 *
 * <p>The locations live under {@code event-outboxer/migration/} — deliberately outside {@code
 * db/migration/}, which Flyway scans recursively for the application's own instance. Keeping the
 * outbox SQL out of that tree is what prevents the library's {@code V001…V007} from colliding with
 * the application's version numbers.
 *
 * <p>Resolution is classpath-driven: {@link #CORE} and {@link #ARCHIVE} always (both ship in {@code
 * event-outboxer-storage-postgres}); {@link #LOCK} whenever {@code
 * event-outboxer-lock-postgres-lease} is present. Each lane touches only its own tables, so the
 * instance runs with {@code outOfOrder} and a lane adopted later (say, the lease locker added after
 * core migrations already ran) applies cleanly.
 */
public final class OutboxFlywayLocations {

    /** {@code events}, {@code workers} and their indexes (V001, V003, V004, V006). */
    public static final String CORE = "classpath:event-outboxer/migration/core";

    /** {@code event_archive} (V002, V007). */
    public static final String ARCHIVE = "classpath:event-outboxer/migration/archive";

    /**
     * {@code entity_locks} lease table (V005) — ships in {@code
     * event-outboxer-lock-postgres-lease}.
     */
    public static final String LOCK = "classpath:event-outboxer/migration/lock";

    static final String LEASE_LOCKER_CLASS =
            "io.github.bams22.outboxer.lock.postgres.lease.PgLeaseEntityLocker";

    private OutboxFlywayLocations() {}

    /**
     * Locations to apply for the given classpath: core and archive always, lock when the lease
     * locker module is present.
     */
    public static List<String> resolve(@Nullable ClassLoader classLoader) {
        List<String> locations = new ArrayList<>(List.of(CORE, ARCHIVE));
        if (ClassUtils.isPresent(LEASE_LOCKER_CLASS, classLoader)) {
            locations.add(LOCK);
        }
        return List.copyOf(locations);
    }
}
