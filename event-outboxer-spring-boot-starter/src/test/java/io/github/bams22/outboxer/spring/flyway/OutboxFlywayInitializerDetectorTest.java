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

import java.util.ArrayList;
import java.util.List;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.sql.init.dependency.DatabaseInitializationDependencyConfigurer;
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * The detector is what orders {@code @DependsOnDatabaseInitialization} beans (the lease-table
 * probe) after the outbox migrations. Boot's {@link DatabaseInitializationDependencyConfigurer} is
 * imported directly here — in an application it arrives through {@code FlywayAutoConfiguration} —
 * and the detector through {@code spring.factories}, exactly as at runtime.
 */
class OutboxFlywayInitializerDetectorTest {

    static final List<String> CREATION_ORDER = new ArrayList<>();

    @Test
    @DisplayName("a @DependsOnDatabaseInitialization bean is created after the outbox initializer")
    void dependentBeanWaitsForOutboxMigration() {
        CREATION_ORDER.clear();
        new ApplicationContextRunner()
                .withUserConfiguration(OrderingConfiguration.class)
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThat(CREATION_ORDER)
                                    .containsExactly("outboxFlyway", "tableProbe");
                            assertThat(
                                            ctx.getBeanFactory()
                                                    .getBeanDefinition("tableProbe")
                                                    .getDependsOn())
                                    .contains("outboxFlyway");
                        });
    }

    @Test
    @DisplayName("the detector reports exactly the initializer beans")
    void detectsInitializerBeans() {
        DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
        beanFactory.registerSingleton("outboxFlyway", stubInitializer("unused"));
        beanFactory.registerSingleton("other", new Object());

        assertThat(new OutboxFlywayInitializerDetector().detect(beanFactory))
                .containsExactly("outboxFlyway");
    }

    static OutboxFlywayMigrationInitializer stubInitializer(String name) {
        return new OutboxFlywayMigrationInitializer(
                Flyway.configure().dataSource(OutboxFlywayFactoryTest.stubDataSource()).load(),
                "event_outboxer") {
            @Override
            public void afterPropertiesSet() {
                CREATION_ORDER.add(name);
            }
        };
    }

    @Configuration(proxyBeanMethods = false)
    @Import(DatabaseInitializationDependencyConfigurer.class)
    static class OrderingConfiguration {

        // Declared first on purpose: without the detector, registration order would create it
        // before the initializer.
        @Bean
        @DependsOnDatabaseInitialization
        Object tableProbe() {
            CREATION_ORDER.add("tableProbe");
            return new Object();
        }

        @Bean
        OutboxFlywayMigrationInitializer outboxFlyway() {
            return stubInitializer("outboxFlyway");
        }
    }
}
