/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.lock;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.InitializingBean;

/**
 * Fail-fast startup probe for the lease locker: verifies {@code <schema>.entity_locks} exists and
 * converts a missed V005 migration into an actionable startup error instead of a first-lock runtime
 * failure (ADR-0022 §Starter integration). Creation is ordered after Flyway/Liquibase via
 * {@code @DependsOnDatabaseInitialization} on the {@code @Bean} method, so the first deploy that
 * adds V005 does not race its own migration.
 */
final class PgLeaseTableProbe implements InitializingBean {

    private final @Nullable DataSource dataSource;
    private final String schema;

    /**
     * A {@code null} dataSource disables the probe — used when a user-defined {@code EntityLocker}
     * displaced the lease locker and the table is not required.
     */
    PgLeaseTableProbe(@Nullable DataSource dataSource, String schema) {
        this.dataSource = dataSource;
        this.schema = schema;
    }

    @Override
    public void afterPropertiesSet() {
        if (dataSource == null) {
            return;
        }
        String sql = "SELECT 1 FROM " + schema + ".entity_locks LIMIT 1";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.executeQuery().close();
        } catch (SQLException ex) {
            throw new IllegalStateException(
                    "event-outboxer.lock.type=postgres-lease requires the "
                            + schema
                            + ".entity_locks lease table (migration V005__outbox_entity_locks.sql,"
                            + " shipped in event-outboxer-lock-postgres-lease). The starter applies"
                            + " it through its own Flyway instance when Flyway is on the classpath"
                            + " and event-outboxer.flyway.enabled is true (ADR-0028); otherwise"
                            + " apply 'classpath:event-outboxer/migration/lock' (or the"
                            + " db/changelog/outbox/lock/changelog.xml Liquibase changelog)"
                            + " yourself and redeploy. To keep the pre-ADR-0022 advisory locker"
                            + " instead, set event-outboxer.lock.type=postgres-advisory.",
                    ex);
        }
    }
}
