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

import io.github.bams22.outboxer.benchmark.ledger.JdbcLedger;
import io.github.bams22.outboxer.benchmark.scenario.Scenario;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Entry point of a forked worker JVM ({@code --bench.role=worker --bench.spec=<file>}). Boots one
 * worker context writing into the database ledger, marks itself ready, then waits: for {@code
 * SIGTERM} (graceful stop through the shutdown hook, claims released) or for the driver to go away
 * (stdin reaches end-of-file when the parent dies), which also stops it. {@code SIGKILL} needs no
 * cooperation.
 */
public final class WorkerProcess {

    private WorkerProcess() {}

    /**
     * Runs the worker described by {@code specFile} until told to stop.
     *
     * @return process exit code
     */
    public static int run(Path specFile) {
        WorkerSpec spec = WorkerSpec.read(specFile);
        Scenario scenario = spec.scenario();
        int ledgerPool = scenario.handlerPoolSize() * scenario.eventTypes() + 1;
        JdbcLedger ledger =
                new JdbcLedger(spec.database(), ledgerPool, "ledger-" + spec.workerId());
        AtomicBoolean closed = new AtomicBoolean();
        ConfigurableApplicationContext ctx;
        try {
            ctx =
                    WorkerBootstrap.boot(
                            spec.database(),
                            spec.redisUri(),
                            scenario,
                            spec.workerId(),
                            false,
                            ledger,
                            false);
        } catch (RuntimeException e) {
            e.printStackTrace(System.err);
            ledger.close();
            return 3;
        }
        Runnable stop =
                () -> {
                    if (closed.compareAndSet(false, true)) {
                        ctx.close();
                        ledger.close();
                    }
                };
        Runtime.getRuntime().addShutdownHook(new Thread(stop, "bench-worker-stop"));
        try {
            Files.writeString(Path.of(spec.readyFile()), spec.workerId());
        } catch (IOException e) {
            e.printStackTrace(System.err);
            stop.run();
            return 3;
        }
        waitForParent();
        stop.run();
        return 0;
    }

    private static void waitForParent() {
        try {
            // Blocks until the driver closes its end of the pipe or exits.
            while (System.in.read() != -1) {
                // Nothing is ever sent; consume and keep waiting.
            }
        } catch (IOException e) {
            // Pipe broken: treat as "parent gone".
        }
    }
}
