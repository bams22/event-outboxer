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

import java.util.Set;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.sql.init.dependency.DatabaseInitializerDetector;
import org.springframework.util.ClassUtils;

/**
 * Marks {@link OutboxFlywayMigrationInitializer} as a database initializer for Spring Boot's
 * dependency configurer, so every bean that is {@code @DependsOnDatabaseInitialization} (the
 * lease-table probe) or detected as depending on database initialization ({@code JdbcTemplate},
 * {@code JdbcClient}, JPA, …) is created after the outbox schema is migrated.
 *
 * <p>Registered through {@code META-INF/spring.factories}; the initializer class is only touched
 * when Flyway is on the classpath, so the detector is safe to load without it.
 */
public class OutboxFlywayInitializerDetector implements DatabaseInitializerDetector {

    private static final String FLYWAY_CLASS = "org.flywaydb.core.Flyway";

    @Override
    public Set<String> detect(ConfigurableListableBeanFactory beanFactory) {
        if (!ClassUtils.isPresent(FLYWAY_CLASS, beanFactory.getBeanClassLoader())) {
            return Set.of();
        }
        return Set.of(
                beanFactory.getBeanNamesForType(
                        OutboxFlywayMigrationInitializer.class, true, false));
    }
}
