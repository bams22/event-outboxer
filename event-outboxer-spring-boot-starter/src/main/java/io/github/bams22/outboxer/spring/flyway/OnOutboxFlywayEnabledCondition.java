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

import org.springframework.boot.autoconfigure.condition.AllNestedConditions;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/**
 * The starter-managed Flyway instance is active when the outbox master switch is on, {@code
 * event-outboxer.flyway.enabled} is not {@code false}, and the PostgreSQL storage adapter is
 * selected — there is nothing to migrate for any other storage.
 */
final class OnOutboxFlywayEnabledCondition extends AllNestedConditions {

    OnOutboxFlywayEnabledCondition() {
        super(ConfigurationPhase.PARSE_CONFIGURATION);
    }

    @ConditionalOnProperty(
            prefix = "event-outboxer",
            name = "enabled",
            havingValue = "true",
            matchIfMissing = true)
    static final class MasterSwitch {}

    @ConditionalOnProperty(
            prefix = "event-outboxer.flyway",
            name = "enabled",
            havingValue = "true",
            matchIfMissing = true)
    static final class FlywaySwitch {}

    @ConditionalOnProperty(
            prefix = "event-outboxer.storage",
            name = "type",
            havingValue = "postgres")
    static final class PostgresStorage {}
}
