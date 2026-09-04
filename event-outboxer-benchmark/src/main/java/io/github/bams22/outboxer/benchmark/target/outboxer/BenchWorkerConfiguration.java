/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.target.outboxer;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * Root configuration of every context the target boots. Nearly empty: the starter's
 * autoconfiguration does the wiring, handlers are registered programmatically per context (one per
 * event type, bound to the shared ledger), and everything else is a property. The one bean here is
 * what an application would bring itself: the {@code RedissonClient} the {@code redisson} locker
 * rides (the starter never creates one, ADR-0036).
 *
 * <p>Spring Boot's own Flyway autoconfiguration is excluded: the outbox schema is migrated by the
 * starter-managed instance (ADR-0028) and the harness has no application migrations of its own.
 */
@SpringBootConfiguration
@EnableAutoConfiguration(exclude = FlywayAutoConfiguration.class)
public class BenchWorkerConfiguration {

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnProperty("bench.redisson.address")
    public RedissonClient benchRedissonClient(@Value("${bench.redisson.address}") String address) {
        Config config = new Config();
        config.useSingleServer().setAddress(address);
        return Redisson.create(config);
    }
}
