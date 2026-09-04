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

import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.benchmark.db.DatabaseCoordinates;
import io.github.bams22.outboxer.benchmark.ledger.Ledger;
import io.github.bams22.outboxer.benchmark.scenario.LockType;
import io.github.bams22.outboxer.benchmark.scenario.Scenario;
import io.github.bams22.outboxer.domain.EventType;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.Banner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;

/**
 * Boots one Spring context of the event-outboxer target — publisher or worker — from a scenario.
 * Shared by the in-process fleet, the forked worker process and the publisher side, so every
 * context of a run is configured identically.
 */
final class WorkerBootstrap {

    /**
     * Schema the starter migrates and the probe samples ({@code event-outboxer.storage.schema}).
     */
    static final String SCHEMA = "event_outboxer";

    private WorkerBootstrap() {}

    /**
     * Boots a context. A worker ({@code publishOnly=false}) registers one handler per event type
     * writing into {@code ledger}; a publisher registers none.
     *
     * @param shutdownHook whether the JVM shutdown hook closes the context (forked workers: yes, so
     *     {@code SIGTERM} is a graceful stop; in-process: no, the fleet closes them)
     */
    static ConfigurableApplicationContext boot(
            DatabaseCoordinates database,
            @Nullable String redisUri,
            Scenario scenario,
            String workerId,
            boolean publishOnly,
            @Nullable Ledger ledger,
            boolean shutdownHook) {
        SpringApplicationBuilder builder =
                new SpringApplicationBuilder(BenchWorkerConfiguration.class)
                        .web(WebApplicationType.NONE)
                        .bannerMode(Banner.Mode.OFF)
                        .logStartupInfo(false)
                        .registerShutdownHook(shutdownHook)
                        .properties(
                                properties(database, redisUri, scenario, workerId, publishOnly));
        // Every context, the publish-only one included: publish-only engines still run
        // maintenance (ADR-0029), so the publisher may be the one reclaiming orphans.
        builder.initializers(
                ctx ->
                        ((GenericApplicationContext) ctx)
                                .registerBean(
                                        "benchRecoveryLogListener",
                                        OutboxListener.class,
                                        () -> new RecoveryLogListener(workerId)));
        if (!publishOnly) {
            if (ledger == null) {
                throw new IllegalArgumentException("A worker context needs a ledger");
            }
            List<EventType<?>> types =
                    BenchPayloads.types(scenario.eventTypes(), scenario.payloadFormat());
            builder.initializers(
                    ctx -> {
                        GenericApplicationContext generic = (GenericApplicationContext) ctx;
                        for (EventType<?> type : types) {
                            generic.registerBean(
                                    "benchHandler_" + type.name(),
                                    EventHandler.class,
                                    () -> BenchPayloads.handler(type, ledger, scenario));
                        }
                    });
        }
        return builder.run();
    }

    /** The starter properties a scenario translates to, plus the scenario's own overrides last. */
    static Map<String, Object> properties(
            DatabaseCoordinates database,
            @Nullable String redisUri,
            Scenario s,
            String workerId,
            boolean publishOnly) {
        if (s.lockType() == LockType.REDIS && redisUri == null) {
            throw new IllegalArgumentException("lock=redis needs a Redis URI");
        }
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("spring.datasource.url", database.jdbcUrl());
        p.put("spring.datasource.username", database.username());
        p.put("spring.datasource.password", database.password());
        p.put("spring.datasource.hikari.pool-name", workerId);
        p.put("spring.datasource.hikari.maximum-pool-size", s.connectionPoolSize());

        p.put("event-outboxer.storage.type", "postgres");
        p.put("event-outboxer.serializer.write-format", s.payloadFormat().property());
        p.put("event-outboxer.storage.schema", SCHEMA);
        p.put("event-outboxer.worker.id", workerId);
        p.put("event-outboxer.publish-only", publishOnly);
        p.put("event-outboxer.lock.type", s.lockType().property());
        if (redisUri != null) {
            p.put("event-outboxer.redis.uri", redisUri);
            p.put("event-outboxer.redis.client-name", workerId);
        }
        p.put("event-outboxer.handler-executor.type", s.executorType().property());
        p.put("event-outboxer.dispatcher.finalize-batching", s.finalizeBatching());
        p.put("event-outboxer.event-types.defaults.handler-pool-size", s.handlerPoolSize());
        p.put("event-outboxer.event-types.defaults.claim-batch-size", s.claimBatchSize());
        p.put("event-outboxer.event-types.defaults.poll-min-interval", millis(s.pollMinInterval()));
        p.put("event-outboxer.event-types.defaults.poll-max-interval", millis(s.pollMaxInterval()));
        p.put("event-outboxer.maintenance.shutdown-timeout", "30s");

        p.put("logging.level.root", "WARN");
        p.putAll(s.workerProperties());
        return p;
    }

    private static String millis(Duration d) {
        return d.toMillis() + "ms";
    }
}
