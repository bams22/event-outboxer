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

import io.github.bams22.outboxer.benchmark.target.BenchmarkEnvironment;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Phase-1 fleet: one Spring context per worker in the driver JVM, all reporting into the shared
 * in-memory ledger. Cannot be killed — an abandoned context would keep finalizing on its handler
 * threads, which is not what a crash looks like.
 */
final class InProcessFleet implements WorkerFleet {

    private final BenchmarkEnvironment env;
    private final Map<String, ConfigurableApplicationContext> workers = new LinkedHashMap<>();
    private int next;

    InProcessFleet(BenchmarkEnvironment env) {
        this.env = env;
    }

    @Override
    public synchronized List<String> start(int count) {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String id = OutboxerTarget.WORKER_ID_PREFIX + next++;
            workers.put(
                    id,
                    WorkerBootstrap.boot(
                            env.database(),
                            env.redisUri(),
                            env.scenario(),
                            id,
                            false,
                            env.ledger(),
                            false));
            ids.add(id);
        }
        return ids;
    }

    @Override
    public synchronized List<String> ids() {
        return List.copyOf(workers.keySet());
    }

    @Override
    public List<String> kill(int count) {
        throw new UnsupportedOperationException(
                "The in-process fleet cannot be killed honestly; use --bench.fleet=forked");
    }

    @Override
    public synchronized void close() {
        for (ConfigurableApplicationContext ctx : workers.values()) {
            ctx.close();
        }
        workers.clear();
    }
}
