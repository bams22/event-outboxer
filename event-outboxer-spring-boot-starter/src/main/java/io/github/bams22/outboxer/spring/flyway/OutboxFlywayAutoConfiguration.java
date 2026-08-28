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

import io.github.bams22.outboxer.spring.OutboxDataSource;
import io.github.bams22.outboxer.spring.OutboxDataSourceResolver;
import io.github.bams22.outboxer.spring.OutboxProperties;
import io.github.bams22.outboxer.storage.postgres.PostgresEventStore;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.io.ResourceLoader;

/**
 * Registers the starter-managed Flyway instance for the outbox schema (ADR-0028).
 *
 * <p>Activates when Flyway and the PostgreSQL storage adapter are on the classpath, {@code
 * event-outboxer.storage.type=postgres}, {@code event-outboxer.flyway.enabled} is not {@code
 * false}, and a connection is available — a {@code DataSource} bean or a dedicated {@code
 * event-outboxer.flyway.url}. The instance is fully self-contained: fixed locations ({@link
 * OutboxFlywayLocations}), its own history table inside {@code event-outboxer.storage.schema}, the
 * schema placeholder pre-filled. Nothing needs to be — or can be — listed in {@code
 * spring.flyway.locations}.
 *
 * <p>Coexists with Spring Boot's {@code FlywayAutoConfiguration}: no {@code Flyway} or {@code
 * FlywayMigrationInitializer} bean is exposed, so the application's own instance is unaffected.
 * Declare an {@link OutboxFlywayMigrationInitializer} bean to take over the wiring entirely.
 */
@AutoConfiguration(after = DataSourceAutoConfiguration.class)
@ConditionalOnClass({Flyway.class, PostgresEventStore.class})
@Conditional({OnOutboxFlywayEnabledCondition.class, OnOutboxFlywayDataSourceCondition.class})
@EnableConfigurationProperties(OutboxProperties.class)
public class OutboxFlywayAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public OutboxFlywayMigrationInitializer outboxFlywayMigrationInitializer(
            OutboxProperties properties,
            @OutboxDataSource ObjectProvider<DataSource> qualifiedDataSources,
            ObjectProvider<DataSource> dataSources,
            ListableBeanFactory beanFactory,
            ResourceLoader resourceLoader) {
        DataSource outboxDataSource = null;
        String url = properties.getFlyway().getUrl();
        if (url == null || url.isBlank()) {
            // Migrations run as their own autocommit statements on a raw connection — never
            // through the transaction-aware proxy the storage adapter uses (ADR-0002).
            outboxDataSource =
                    OutboxDataSourceResolver.unwrapTransactionAware(
                            OutboxDataSourceResolver.resolve(
                                    qualifiedDataSources, dataSources, beanFactory));
        }
        Flyway flyway =
                OutboxFlywayFactory.create(
                        properties, outboxDataSource, classLoaderOf(resourceLoader));
        return new OutboxFlywayMigrationInitializer(flyway, properties.getStorage().getSchema());
    }

    private static @Nullable ClassLoader classLoaderOf(ResourceLoader resourceLoader) {
        return resourceLoader.getClassLoader();
    }
}
