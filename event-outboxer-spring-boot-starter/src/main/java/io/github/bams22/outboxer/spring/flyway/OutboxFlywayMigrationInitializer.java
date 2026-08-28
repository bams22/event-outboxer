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

import java.sql.SQLException;
import java.util.Objects;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.output.MigrateResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.Ordered;

/**
 * Runs the starter-managed outbox migrations during context refresh (ADR-0028). Modelled on Spring
 * Boot's {@code FlywayMigrationInitializer} but deliberately a distinct type: Boot's own {@code
 * flywayInitializer} bean is {@code @ConditionalOnMissingBean}, and sharing its type would make the
 * application's migrations silently disappear.
 *
 * <p>Beans that read the outbox tables during startup (the lease-table probe, {@code
 * JdbcTemplate}s, …) are ordered after this bean through {@link OutboxFlywayInitializerDetector}.
 *
 * <p>A non-empty outbox schema without a history table of its own is the signature of an
 * installation that applied the outbox migrations through the application's Flyway instance before
 * this one existed. Flyway refuses to migrate such a schema ({@code Found non-empty schema(s) … but
 * no schema history table}); that refusal — and, as a fallback, a {@code relation already exists}
 * SQL error — is rethrown with the baseline recipe instead of the raw message.
 */
public class OutboxFlywayMigrationInitializer implements InitializingBean, Ordered {

    private static final Logger log =
            LoggerFactory.getLogger(OutboxFlywayMigrationInitializer.class);

    /** PostgreSQL {@code duplicate_table} / {@code duplicate_object}. */
    private static final String DUPLICATE_TABLE = "42P07";

    private static final String DUPLICATE_OBJECT = "42710";

    private final Flyway flyway;
    private final String schema;

    /**
     * Creates the initializer.
     *
     * @param flyway the loaded outbox instance (see {@link OutboxFlywayFactory})
     * @param schema the outbox schema, used only for log and error messages
     */
    public OutboxFlywayMigrationInitializer(Flyway flyway, String schema) {
        this.flyway = Objects.requireNonNull(flyway, "flyway must not be null");
        this.schema = Objects.requireNonNull(schema, "schema must not be null");
    }

    @Override
    public void afterPropertiesSet() {
        MigrateResult result;
        try {
            result = flyway.migrate();
        } catch (FlywayException ex) {
            if (isNonEmptySchemaWithoutHistory(ex) || isPreexistingObject(ex)) {
                throw new IllegalStateException(upgradeHint(), ex);
            }
            throw ex;
        }
        if (result.migrationsExecuted > 0) {
            log.info(
                    "outbox schema '{}': applied {} migration(s), now at version {}",
                    schema,
                    result.migrationsExecuted,
                    result.targetSchemaVersion);
        } else {
            log.debug("outbox schema '{}' is up to date", schema);
        }
    }

    @Override
    public int getOrder() {
        return 0;
    }

    /** The loaded instance, exposed for diagnostics and tests. */
    public Flyway getFlyway() {
        return flyway;
    }

    private String upgradeHint() {
        return "The outbox schema '"
                + schema
                + "' already contains tables but no Flyway history of its own. This is the shape"
                + " of an installation that applied the outbox migrations through the"
                + " application's Flyway instance (spring.flyway.locations) before the starter"
                + " ran them itself (ADR-0028). For one deploy set"
                + " event-outboxer.flyway.baseline-on-migrate=true and"
                + " event-outboxer.flyway.baseline-version=<highest outbox migration already"
                + " applied — 7 if you had every location> so the existing objects are recorded as"
                + " applied; then remove both properties. Also drop the outbox locations from"
                + " spring.flyway.locations and set spring.flyway.ignore-migration-patterns to"
                + " '*:missing' (or delete the outbox rows from your history table) — see the"
                + " CHANGELOG upgrade notes.";
    }

    private static boolean isNonEmptySchemaWithoutHistory(FlywayException ex) {
        String message = ex.getMessage();
        return message != null
                && message.contains("non-empty schema")
                && message.contains("no schema history table");
    }

    private static boolean isPreexistingObject(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof SQLException sql) {
                String state = sql.getSQLState();
                if (DUPLICATE_TABLE.equals(state) || DUPLICATE_OBJECT.equals(state)) {
                    return true;
                }
            }
        }
        return false;
    }
}
