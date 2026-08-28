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

import io.github.bams22.outboxer.spring.OutboxProperties;
import java.util.Map;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.util.ClassUtils;

/**
 * Builds the starter-managed {@link Flyway} instance for the outbox schema (ADR-0028). Kept apart
 * from the auto-configuration so the resulting configuration — locations, schema, placeholder,
 * connection — can be asserted without a database.
 *
 * <h2>Shape of the instance</h2>
 *
 * <ul>
 *   <li>locations: {@link OutboxFlywayLocations#resolve(ClassLoader)} — fixed, never read from
 *       properties;
 *   <li>{@code schemas} = {@code event-outboxer.storage.schema}: Flyway creates the schema when
 *       missing and keeps its own {@code flyway_schema_history} inside it, so the application's
 *       history table is never touched;
 *   <li>placeholder {@code eventOutboxerSchema} = the same schema — the single source of truth for
 *       the adapter's SQL and the DDL;
 *   <li>{@code outOfOrder = true}: the shipped lanes (core / archive / lock) touch disjoint tables,
 *       so a lane adopted later applies without a validation failure;
 *   <li>{@code baselineOnMigrate} / {@code baselineVersion} pass through from {@code
 *       event-outboxer.flyway.*} for the one-time upgrade from installations that migrated through
 *       the application's Flyway instance.
 * </ul>
 *
 * <h2>Connection</h2>
 *
 * {@code event-outboxer.flyway.url} builds a dedicated {@link SimpleDriverDataSource} (user /
 * password / driver-class-name optional). Without a URL the outbox {@code DataSource} is used;
 * {@code user} alone derives a connection from it with the given credentials — the same precedence
 * Spring Boot applies to {@code spring.flyway.url} / {@code user} / {@code password}.
 */
public final class OutboxFlywayFactory {

    static final String SCHEMA_PLACEHOLDER = "eventOutboxerSchema";

    /**
     * Flyway 10+ ships PostgreSQL support as a plugin module; Flyway 9 had it built in. Either
     * class proves the driver-side support is present.
     */
    private static final String[] POSTGRES_DATABASE_TYPES = {
        "org.flywaydb.database.postgresql.PostgreSQLDatabaseType",
        "org.flywaydb.core.internal.database.postgresql.PostgreSQLDatabaseType"
    };

    private OutboxFlywayFactory() {}

    /**
     * Creates the loaded, ready-to-migrate instance.
     *
     * @param properties root properties ({@code storage.schema} and {@code flyway.*} are read)
     * @param outboxDataSource the resolved outbox {@code DataSource}, or {@code null} when {@code
     *     event-outboxer.flyway.url} designates a dedicated connection
     * @param classLoader class loader for location scanning and driver lookup
     */
    public static Flyway create(
            OutboxProperties properties,
            @Nullable DataSource outboxDataSource,
            @Nullable ClassLoader classLoader) {
        ClassLoader loader =
                classLoader != null ? classLoader : OutboxFlywayFactory.class.getClassLoader();
        requirePostgresSupport(loader);
        OutboxProperties.Flyway flyway = properties.getFlyway();
        String schema = properties.getStorage().getSchema();
        return Flyway.configure(loader)
                .dataSource(migrationDataSource(flyway, outboxDataSource, loader))
                .locations(OutboxFlywayLocations.resolve(loader).toArray(String[]::new))
                .schemas(schema)
                .placeholders(Map.of(SCHEMA_PLACEHOLDER, schema))
                .outOfOrder(true)
                .baselineOnMigrate(flyway.isBaselineOnMigrate())
                .baselineVersion(flyway.getBaselineVersion())
                .load();
    }

    private static DataSource migrationDataSource(
            OutboxProperties.Flyway flyway,
            @Nullable DataSource outboxDataSource,
            ClassLoader classLoader) {
        String url = flyway.getUrl();
        String user = flyway.getUser();
        String password = flyway.getPassword();
        if (url != null && !url.isBlank()) {
            DataSourceBuilder<?> builder =
                    DataSourceBuilder.create(classLoader)
                            .type(SimpleDriverDataSource.class)
                            .url(url);
            if (user != null) {
                builder.username(user);
            }
            if (password != null) {
                builder.password(password);
            }
            String driverClassName = flyway.getDriverClassName();
            if (driverClassName != null && !driverClassName.isBlank()) {
                builder.driverClassName(driverClassName);
            }
            return builder.build();
        }
        if (outboxDataSource == null) {
            throw new IllegalStateException(
                    "The outbox Flyway instance has no connection: no DataSource bean is available"
                            + " and event-outboxer.flyway.url is not set.");
        }
        if (user != null) {
            DataSourceBuilder<?> builder =
                    DataSourceBuilder.derivedFrom(outboxDataSource)
                            .type(SimpleDriverDataSource.class)
                            .username(user);
            if (password != null) {
                builder.password(password);
            }
            return builder.build();
        }
        return outboxDataSource;
    }

    private static void requirePostgresSupport(ClassLoader classLoader) {
        for (String type : POSTGRES_DATABASE_TYPES) {
            if (ClassUtils.isPresent(type, classLoader)) {
                return;
            }
        }
        throw new IllegalStateException(
                "Flyway is on the classpath but its PostgreSQL support is not: add"
                        + " org.flywaydb:flyway-database-postgresql (Flyway 10+ ships each database"
                        + " as a separate module). To apply the outbox migrations yourself instead,"
                        + " set event-outboxer.flyway.enabled=false.");
    }
}
