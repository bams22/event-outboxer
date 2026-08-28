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

import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.condition.AnyNestedCondition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/**
 * The outbox Flyway instance needs a connection: either a dedicated {@code
 * event-outboxer.flyway.url} or a {@code DataSource} bean to migrate through. Without both the
 * auto-configuration backs off and the storage failure analyzer explains the missing {@code
 * DataSource}.
 */
final class OnOutboxFlywayDataSourceCondition extends AnyNestedCondition {

    OnOutboxFlywayDataSourceCondition() {
        super(ConfigurationPhase.REGISTER_BEAN);
    }

    @ConditionalOnProperty(prefix = "event-outboxer.flyway", name = "url")
    static final class DedicatedUrl {}

    @ConditionalOnBean(DataSource.class)
    static final class DataSourceBean {}
}
