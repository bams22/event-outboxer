/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.lifecycle;

import io.github.bams22.outboxer.core.engine.OutboxEngine;
import org.springframework.context.SmartLifecycle;

/**
 * Spring {@link SmartLifecycle} wrapper around {@link OutboxEngine}. Runs at phase 20000 — late
 * enough that DataSource, connection pools, and Flyway migrations are already up, early enough that
 * the engine has time to publish metrics before the actuator reports readiness.
 *
 * <p>Per {@code SmartLifecycle} contract: {@code start()} fires on context refresh ordered by phase
 * ascending; {@code stop(Runnable)} fires on context close ordered by phase descending. The
 * engine's own {@code stop(Duration)} waits for in-flight handlers to drain.
 */
public final class OutboxSmartLifecycle implements SmartLifecycle {

    /** Started after infrastructure but before application controllers / message listeners. */
    public static final int PHASE = 20_000;

    private final OutboxEngine engine;

    public OutboxSmartLifecycle(OutboxEngine engine) {
        this.engine = engine;
    }

    @Override
    public void start() {
        // Use isLifecycleActive() rather than state() == STOPPED so a crashed engine (state()
        // reports
        // STOPPED, but isLifecycleActive() still true until stop() completes) does not get
        // double-started.
        if (!engine.isLifecycleActive()) {
            engine.start();
        }
    }

    @Override
    public void stop() {
        engine.stop();
    }

    @Override
    public void stop(Runnable callback) {
        try {
            engine.stop();
        } finally {
            callback.run();
        }
    }

    @Override
    public boolean isRunning() {
        // Return true across the entire lifecycle window (start-to-stop), ignoring crashes. This
        // guarantees Spring Boot still calls stop() on a crashed engine so the normal cleanup
        // (worker deregister, handler drain, graceful_stop flag) runs.
        return engine.isLifecycleActive();
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }
}
