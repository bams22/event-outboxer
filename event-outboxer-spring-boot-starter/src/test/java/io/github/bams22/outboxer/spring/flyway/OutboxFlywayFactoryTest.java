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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariDataSource;
import io.github.bams22.outboxer.spring.OutboxProperties;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.Location;
import org.flywaydb.core.api.configuration.Configuration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

class OutboxFlywayFactoryTest {

    private static final String PG_DATABASE_TYPE =
            "org.flywaydb.database.postgresql.PostgreSQLDatabaseType";

    @Test
    @DisplayName("locations, schema, placeholder and out-of-order are fixed by the starter")
    void fixedShape() {
        OutboxProperties props = new OutboxProperties();
        props.getStorage().setSchema("my_outbox");

        Configuration cfg =
                OutboxFlywayFactory.create(props, stubDataSource(), getClass().getClassLoader())
                        .getConfiguration();

        // Flyway keeps locations sorted; order is irrelevant for disjoint lanes.
        assertThat(cfg.getLocations())
                .extracting(Location::getDescriptor)
                .containsExactlyInAnyOrderElementsOf(
                        OutboxFlywayLocations.resolve(getClass().getClassLoader()));
        assertThat(cfg.getSchemas()).containsExactly("my_outbox");
        assertThat(cfg.getPlaceholders()).containsEntry("eventOutboxerSchema", "my_outbox");
        assertThat(cfg.isOutOfOrder()).isTrue();
        assertThat(cfg.isBaselineOnMigrate()).isFalse();
        assertThat(cfg.getBaselineVersion().getVersion()).isEqualTo("1");
    }

    @Test
    @DisplayName("baseline properties pass through for the one-time upgrade")
    void baselinePassThrough() {
        OutboxProperties props = new OutboxProperties();
        props.getFlyway().setBaselineOnMigrate(true);
        props.getFlyway().setBaselineVersion("7");

        Configuration cfg =
                OutboxFlywayFactory.create(props, stubDataSource(), getClass().getClassLoader())
                        .getConfiguration();

        assertThat(cfg.isBaselineOnMigrate()).isTrue();
        assertThat(cfg.getBaselineVersion().getVersion()).isEqualTo("7");
    }

    @Test
    @DisplayName("without url the outbox DataSource is used as is")
    void outboxDataSourceByDefault() {
        DataSource outbox = stubDataSource();

        Flyway flyway =
                OutboxFlywayFactory.create(
                        new OutboxProperties(), outbox, getClass().getClassLoader());

        assertThat(flyway.getConfiguration().getDataSource()).isSameAs(outbox);
    }

    @Test
    @DisplayName("url builds a dedicated SimpleDriverDataSource with the given credentials")
    void dedicatedUrl() {
        OutboxProperties props = new OutboxProperties();
        props.getFlyway().setUrl("jdbc:postgresql://db.internal:5432/orders");
        props.getFlyway().setUser("migrator");
        props.getFlyway().setPassword("secret");

        DataSource ds =
                OutboxFlywayFactory.create(props, null, getClass().getClassLoader())
                        .getConfiguration()
                        .getDataSource();

        assertThat(ds)
                .isInstanceOfSatisfying(
                        SimpleDriverDataSource.class,
                        simple -> {
                            assertThat(simple.getUrl())
                                    .isEqualTo("jdbc:postgresql://db.internal:5432/orders");
                            assertThat(simple.getUsername()).isEqualTo("migrator");
                            assertThat(simple.getPassword()).isEqualTo("secret");
                            assertThat(simple.getDriver())
                                    .isInstanceOf(org.postgresql.Driver.class);
                        });
    }

    @Test
    @DisplayName("user without url derives a connection from the outbox DataSource")
    void derivedCredentials() {
        HikariDataSource outbox = new HikariDataSource();
        outbox.setJdbcUrl("jdbc:postgresql://db.internal:5432/orders");
        outbox.setUsername("app");
        outbox.setPassword("app-secret");
        OutboxProperties props = new OutboxProperties();
        props.getFlyway().setUser("migrator");
        props.getFlyway().setPassword("migrator-secret");

        DataSource ds =
                OutboxFlywayFactory.create(props, outbox, getClass().getClassLoader())
                        .getConfiguration()
                        .getDataSource();

        assertThat(ds)
                .isInstanceOfSatisfying(
                        SimpleDriverDataSource.class,
                        simple -> {
                            assertThat(simple.getUrl())
                                    .isEqualTo("jdbc:postgresql://db.internal:5432/orders");
                            assertThat(simple.getUsername()).isEqualTo("migrator");
                            assertThat(simple.getPassword()).isEqualTo("migrator-secret");
                        });
    }

    @Test
    @DisplayName("no url and no DataSource is an actionable error")
    void noConnection() {
        assertThatThrownBy(
                        () ->
                                OutboxFlywayFactory.create(
                                        new OutboxProperties(), null, getClass().getClassLoader()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("event-outboxer.flyway.url");
    }

    @Test
    @DisplayName("Flyway without its PostgreSQL module fails fast naming the artifact")
    void missingPostgresModule() {
        ClassLoader withoutPgModule =
                new FilteredClassLoader(name -> name.equals(PG_DATABASE_TYPE));

        assertThatThrownBy(
                        () ->
                                OutboxFlywayFactory.create(
                                        new OutboxProperties(), stubDataSource(), withoutPgModule))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("flyway-database-postgresql")
                .hasMessageContaining("event-outboxer.flyway.enabled=false");
    }

    /** A {@link DataSource} that is never connected to — the factory must not open a connection. */
    static DataSource stubDataSource() {
        return stubDataSource("stub DataSource must not be connected");
    }

    /**
     * A {@link DataSource} whose {@code getConnection} fails with the given message, so a test can
     * tell which of several stubs a context tried to migrate through.
     */
    static DataSource stubDataSource(String failureMessage) {
        return (DataSource)
                Proxy.newProxyInstance(
                        OutboxFlywayFactoryTest.class.getClassLoader(),
                        new Class<?>[] {DataSource.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("getConnection")) {
                                throw new SQLException(failureMessage);
                            }
                            if (method.getName().equals("toString")) {
                                return "stub DataSource";
                            }
                            if (method.getName().equals("hashCode")) {
                                return System.identityHashCode(proxy);
                            }
                            if (method.getName().equals("equals")) {
                                return proxy == args[0];
                            }
                            return null;
                        });
    }
}
