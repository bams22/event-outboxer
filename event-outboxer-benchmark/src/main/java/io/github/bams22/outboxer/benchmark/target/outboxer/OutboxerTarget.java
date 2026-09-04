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

import io.github.bams22.outboxer.api.publish.OutboxEventPublisher;
import io.github.bams22.outboxer.benchmark.scenario.FleetMode;
import io.github.bams22.outboxer.benchmark.target.BenchmarkEnvironment;
import io.github.bams22.outboxer.benchmark.target.BenchmarkPublisher;
import io.github.bams22.outboxer.benchmark.target.BenchmarkTarget;
import io.github.bams22.outboxer.benchmark.target.TargetSession;
import io.github.bams22.outboxer.domain.EventType;
import java.util.List;
import java.util.Objects;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The library as a team deploys it: the Spring Boot starter over PostgreSQL. One publish-only
 * context (ADR-0029) serves the driver; each worker is an independent instance with its own
 * explicit worker id, connection pool, executors and pollers — a Spring context in this JVM ({@link
 * FleetMode#IN_PROCESS}) or a forked JVM ({@link FleetMode#FORKED}). Instances share nothing but
 * the database, in particular not the same-JVM poller wake hub, so workers discover events by
 * polling exactly as separate pods do.
 */
public final class OutboxerTarget implements BenchmarkTarget {

    /**
     * Schema the starter migrates and the probe samples ({@code event-outboxer.storage.schema}).
     */
    public static final String SCHEMA = WorkerBootstrap.SCHEMA;

    static final String WORKER_ID_PREFIX = "bench-w";
    private static final String PUBLISHER_ID = "bench-publisher";

    @Override
    public String name() {
        return "event-outboxer";
    }

    @Override
    public String storageSchema() {
        return SCHEMA;
    }

    @Override
    public String eventsTable() {
        return SCHEMA + ".events";
    }

    @Override
    public String leaseTable() {
        return SCHEMA + ".entity_locks";
    }

    @Override
    public TargetSession open(BenchmarkEnvironment environment) {
        Objects.requireNonNull(environment, "environment must not be null");
        // Booting the publisher first also runs the starter-managed migrations once, before the
        // fleet races for the Flyway lock.
        ConfigurableApplicationContext publisher =
                WorkerBootstrap.boot(
                        environment.database(),
                        environment.redisUri(),
                        environment.scenario(),
                        PUBLISHER_ID,
                        true,
                        null,
                        false);
        WorkerFleet fleet =
                environment.scenario().fleet() == FleetMode.FORKED
                        ? new ForkedFleet(environment)
                        : new InProcessFleet(environment);
        return new Session(environment, publisher, fleet);
    }

    /** One open target: the publisher context plus the fleet. */
    private static final class Session implements TargetSession {

        private final BenchmarkEnvironment env;
        private final ConfigurableApplicationContext publisherContext;
        private final WorkerFleet fleet;
        private final BenchmarkPublisher publisher;
        private boolean started;

        Session(
                BenchmarkEnvironment env,
                ConfigurableApplicationContext publisherContext,
                WorkerFleet fleet) {
            this.env = env;
            this.publisherContext = publisherContext;
            this.fleet = fleet;
            OutboxEventPublisher outbox = publisherContext.getBean(OutboxEventPublisher.class);
            TransactionTemplate tx =
                    new TransactionTemplate(
                            publisherContext.getBean(PlatformTransactionManager.class));
            List<EventType<?>> types =
                    BenchPayloads.types(
                            env.scenario().eventTypes(), env.scenario().payloadFormat());
            this.publisher =
                    event ->
                            tx.executeWithoutResult(
                                    status ->
                                            BenchPayloads.publish(
                                                    outbox, types.get(event.typeIndex()), event));
        }

        @Override
        public BenchmarkPublisher publisher() {
            return publisher;
        }

        @Override
        public synchronized void startWorkers() {
            if (started) {
                throw new IllegalStateException("Workers already started");
            }
            started = true;
            fleet.start(env.scenario().workers());
        }

        @Override
        public int workerCount() {
            return fleet.ids().size();
        }

        @Override
        public List<String> workerIds() {
            return fleet.ids();
        }

        @Override
        public List<String> killWorkers(int count) {
            return fleet.kill(count);
        }

        @Override
        public List<String> spawnWorkers(int count) {
            return fleet.start(count);
        }

        @Override
        public synchronized void close() {
            // Workers first, so their graceful stop (release of in-flight claims) happens while
            // the database is certainly still reachable; the publisher goes last.
            fleet.close();
            publisherContext.close();
        }
    }
}
