/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

class OutboxLiquibaseParameterEnvironmentPostProcessorTest {

    private final OutboxLiquibaseParameterEnvironmentPostProcessor epp =
            new OutboxLiquibaseParameterEnvironmentPostProcessor();
    private final SpringApplication app = new SpringApplication();

    @Test
    void seedsDefaultSchema_whenOutboxSchemaNotSet() {
        StandardEnvironment env = new StandardEnvironment();

        epp.postProcessEnvironment(env, app);

        assertThat(env.getProperty("spring.liquibase.parameters.eventOutboxerSchema"))
                .isEqualTo("event_outboxer");
    }

    @Test
    void propagatesUserSchema_fromOutboxStorageSchema() {
        StandardEnvironment env = envWith(Map.of("event-outboxer.storage.schema", "my_outbox"));

        epp.postProcessEnvironment(env, app);

        assertThat(env.getProperty("spring.liquibase.parameters.eventOutboxerSchema"))
                .isEqualTo("my_outbox");
    }

    @Test
    void userLiquibaseParameterWins_overOurDefault() {
        StandardEnvironment env =
                envWith(
                        Map.of(
                                "event-outboxer.storage.schema", "my_outbox",
                                "spring.liquibase.parameters.eventOutboxerSchema",
                                        "explicit_override"));

        epp.postProcessEnvironment(env, app);

        assertThat(env.getProperty("spring.liquibase.parameters.eventOutboxerSchema"))
                .isEqualTo("explicit_override");
    }

    private static StandardEnvironment envWith(Map<String, Object> props) {
        StandardEnvironment env = new StandardEnvironment();
        env.getPropertySources().addFirst(new MapPropertySource("user", props));
        return env;
    }
}
