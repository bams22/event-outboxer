/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.engine;

import io.github.bams22.outboxer.core.config.EventTypeConfig;
import io.github.bams22.outboxer.core.polling.HandlerExecutorGate;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns the per-event-type handler executors on behalf of {@link OutboxEngine}. Executors are
 * created on {@link #start()} and drained on {@link #drain(Duration)} so that the engine can be
 * stopped and started again — a fresh {@code start()} installs fresh pools instead of reusing the
 * shut-down ones.
 *
 * <p>Each per-type slot doubles as the poller-facing {@link HandlerExecutorGate}: it tracks the
 * number of in-flight tasks against a capacity budget of {@code handlerPoolSize +
 * handlerQueueCapacity} and reports the edge where free capacity reaches the type's {@code
 * claimMinFree} refill threshold (the saturated→free transition with the default of 1), so the
 * poller claims only what the executor can actually absorb and wakes up the moment a refill is due.
 * For the virtual-thread executor flavour the same budget acts as a soft in-flight cap (the
 * underlying executor itself is unbounded).
 *
 * <p>In-flight accounting is generation-scoped: every {@code start()} creates a fresh counter
 * captured by the tasks submitted to that generation's pool, so a handler that outlives a
 * stop/start cycle decrements its own generation, never the new one.
 */
public final class HandlerExecutorManager {

    private static final Logger log = LoggerFactory.getLogger(HandlerExecutorManager.class);

    /** Total budget for waiting out the synchronous-handoff lag before giving up. */
    private static final long HANDOFF_RETRY_BUDGET_NANOS = 50_000_000L; // 50ms

    /** Pause between handoff retries. */
    private static final long HANDOFF_RETRY_PAUSE_NANOS = 1_000_000L; // 1ms

    private final Function<EventTypeConfig, ExecutorService> factory;
    private final Map<String, EventTypeConfig> configs;
    private final Map<String, Slot> slots;

    public HandlerExecutorManager(
            Function<EventTypeConfig, ExecutorService> factory,
            Map<String, EventTypeConfig> configs) {
        this.factory = Objects.requireNonNull(factory, "factory must not be null");
        this.configs = Map.copyOf(Objects.requireNonNull(configs, "configs must not be null"));
        Map<String, Slot> s = new LinkedHashMap<>();
        for (Map.Entry<String, EventTypeConfig> e : configs.entrySet()) {
            EventTypeConfig cfg = e.getValue();
            s.put(
                    e.getKey(),
                    new Slot(
                            e.getKey(),
                            cfg.handlerPoolSize() + cfg.handlerQueueCapacity(),
                            cfg.claimMinFree()));
        }
        this.slots = s;
    }

    /**
     * Stable per-type gate handed to the poller. Safe to use before {@link #start()} has run
     * (submissions are rejected, {@code freeCapacity()} is zero).
     */
    public HandlerExecutorGate executorFor(String eventType) {
        Slot slot = slots.get(eventType);
        if (slot == null) {
            throw new IllegalArgumentException(
                    "no handler executor configured for type " + eventType);
        }
        return slot;
    }

    /**
     * Free submission capacity of the given type's executor, or {@code 0} while the engine is
     * stopped. Read-only companion to {@link #executorFor(String)} for gauges.
     */
    public int freeCapacity(String eventType) {
        return executorFor(eventType).freeCapacity();
    }

    /**
     * Total capacity budget of the given type's executor: {@code handlerPoolSize +
     * handlerQueueCapacity}. Constant for the manager's lifetime, independent of engine state.
     */
    public int capacity(String eventType) {
        Slot slot = slots.get(eventType);
        if (slot == null) {
            throw new IllegalArgumentException(
                    "no handler executor configured for type " + eventType);
        }
        return slot.capacityLimit;
    }

    /**
     * Create and install a fresh {@code ExecutorService} (and a fresh in-flight generation) per
     * event type.
     */
    public synchronized void start() {
        for (Map.Entry<String, Slot> e : slots.entrySet()) {
            EventTypeConfig cfg =
                    Objects.requireNonNull(
                            configs.get(e.getKey()), "no config for event type " + e.getKey());
            e.getValue().install(factory.apply(cfg));
        }
    }

    /**
     * Drain all executors: reject new work, wait up to {@code timeout} for in-flight handlers, then
     * force-shutdown whatever is left. After this call the gates reject submissions and report zero
     * capacity until the next {@link #start()}.
     */
    public synchronized void drain(Duration timeout) {
        for (Slot slot : slots.values()) {
            Generation gen = slot.uninstall();
            if (gen != null) {
                gen.executor.shutdown();
            }
        }
        long deadline = System.nanoTime() + timeout.toNanos();
        for (Slot slot : slots.values()) {
            Generation gen = slot.drained;
            if (gen == null) {
                continue;
            }
            long remaining = Math.max(0, deadline - System.nanoTime());
            try {
                if (!gen.executor.awaitTermination(remaining, TimeUnit.NANOSECONDS)) {
                    // Queued-but-never-started tasks are dropped here and will never run their
                    // finally
                    // blocks — compensate their increments so the generation's counter stays
                    // truthful
                    // for any still-running abandoned handlers.
                    List<Runnable> dropped = gen.executor.shutdownNow();
                    gen.inFlight.addAndGet(-dropped.size());
                }
            } catch (InterruptedException e) {
                List<Runnable> dropped = gen.executor.shutdownNow();
                gen.inFlight.addAndGet(-dropped.size());
                Thread.currentThread().interrupt();
            }
            slot.drained = null;
        }
    }

    /** One installed executor plus the in-flight counter scoped to its lifetime. */
    private static final class Generation {

        final ExecutorService executor;
        final AtomicInteger inFlight = new AtomicInteger();

        Generation(ExecutorService executor) {
            this.executor = executor;
        }
    }

    /**
     * Mutable holder pollers submit through and observe capacity on. {@code current} is the live
     * generation (or {@code null} while the engine is stopped); {@code drained} keeps the shut-down
     * generation referenced between the reject-new-work and await-termination phases of {@link
     * #drain(Duration)}.
     */
    private final class Slot implements HandlerExecutorGate {

        private final String eventType;
        private final int capacityLimit;
        private final int wakeThreshold;
        private volatile @Nullable Generation current;
        private @Nullable Generation drained;
        private volatile @Nullable Runnable capacityCallback;

        private Slot(String eventType, int capacityLimit, int wakeThreshold) {
            this.eventType = eventType;
            this.capacityLimit = capacityLimit;
            this.wakeThreshold = wakeThreshold;
        }

        private void install(ExecutorService exec) {
            current = new Generation(exec);
        }

        private @Nullable Generation uninstall() {
            Generation gen = current;
            current = null;
            drained = gen;
            return gen;
        }

        @Override
        public int freeCapacity() {
            Generation gen = current;
            if (gen == null) {
                return 0;
            }
            return Math.max(0, capacityLimit - gen.inFlight.get());
        }

        @Override
        public void onCapacityAvailable(Runnable callback) {
            this.capacityCallback = Objects.requireNonNull(callback, "callback must not be null");
        }

        @Override
        public void execute(Runnable command) {
            Generation gen = current;
            if (gen == null) {
                throw new RejectedExecutionException(
                        "handler executor for type " + eventType + " is not running");
            }
            gen.inFlight.incrementAndGet();
            Runnable wrapped =
                    () -> {
                        try {
                            command.run();
                        } finally {
                            completeTask(gen);
                        }
                    };
            try {
                submitWithHandoffRetry(gen, wrapped);
            } catch (RejectedExecutionException ex) {
                gen.inFlight.decrementAndGet();
                throw ex;
            }
        }

        /**
         * The in-flight counter is the capacity source of truth, but with a synchronous handoff
         * (queue capacity 0) there is an inherent lag between a task's finally-block decrement and
         * its worker thread returning to {@code queue.take()} — a submit inside that window is
         * rejected even though capacity logically exists. Since callers only submit within the
         * counter's budget, briefly waiting out the lag and retrying is correct, not optimistic:
         * the worker arrives within microseconds. A submission still rejected after the budget
         * (shutdown race, a user-supplied executor with different semantics) propagates to the
         * caller's rejection safety net.
         */
        private void submitWithHandoffRetry(Generation gen, Runnable wrapped) {
            long deadline = System.nanoTime() + HANDOFF_RETRY_BUDGET_NANOS;
            while (true) {
                try {
                    gen.executor.execute(wrapped);
                    return;
                } catch (RejectedExecutionException ex) {
                    if (gen != current
                            || gen.executor.isShutdown()
                            || System.nanoTime() >= deadline) {
                        throw ex;
                    }
                    LockSupport.parkNanos(HANDOFF_RETRY_PAUSE_NANOS);
                }
            }
        }

        private void completeTask(Generation gen) {
            int remaining = gen.inFlight.decrementAndGet();
            // Fire only on the edge where free capacity reaches the poller's refill threshold
            // (claimMinFree; the saturated→free edge when it is 1), and only for the LIVE
            // generation — completions of abandoned tasks from a drained generation must not wake
            // anyone. Completions decrement one at a time, so the equality check cannot skip the
            // edge.
            if (remaining == capacityLimit - wakeThreshold && gen == current) {
                Runnable callback = capacityCallback;
                if (callback != null) {
                    try {
                        callback.run();
                    } catch (RuntimeException ex) {
                        log.debug(
                                "capacity-available callback failed for type {}: {}",
                                eventType,
                                ex.toString());
                    }
                }
            }
        }
    }
}
