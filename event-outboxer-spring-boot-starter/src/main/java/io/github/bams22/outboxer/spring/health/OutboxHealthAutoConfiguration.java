/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.health;

import io.github.bams22.outboxer.core.engine.OutboxEngine;
import io.github.bams22.outboxer.spi.EventStore;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/** Registers {@link OutboxHealthIndicator} when Spring Boot Actuator is on the classpath. */
@AutoConfiguration
@ConditionalOnClass(HealthIndicator.class)
public class OutboxHealthAutoConfiguration {

    @Bean("outboxHealthIndicator")
    @ConditionalOnMissingBean(name = "outboxHealthIndicator")
    public OutboxHealthIndicator outboxHealthIndicator(OutboxEngine engine, EventStore store) {
        return new OutboxHealthIndicator(engine, store);
    }
}
