/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.executor;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Exposes a Spring {@link ThreadPoolTaskExecutor} as a {@link ExecutorService} so the core engine —
 * which accepts only {@code java.util.concurrent} types — can drive it.
 *
 * <p>The point of routing through {@code ThreadPoolTaskExecutor} rather than submitting directly to
 * its underlying {@link ThreadPoolExecutor} is the {@code TaskDecorator}: the decorator hooks in on
 * {@code TPTE.execute()} / {@code TPTE.submit()} and captures MDC / Micrometer Observation /
 * security context from the submitting thread so the handler runs with the same context. Submitting
 * to the underlying pool directly would bypass it.
 *
 * <p>Lifecycle methods ({@code shutdown}, {@code awaitTermination}, {@code shutdownNow}) and the
 * {@code invokeAll} / {@code invokeAny} family delegate to the underlying {@code
 * ThreadPoolExecutor} — these don't need decoration (they are used for pool teardown, not for task
 * dispatch) and {@code ThreadPoolTaskExecutor} doesn't expose them directly.
 */
final class SpringTaskExecutorAdapter implements ExecutorService {

    private final ThreadPoolTaskExecutor taskExecutor;
    private final ThreadPoolExecutor underlying;

    SpringTaskExecutorAdapter(ThreadPoolTaskExecutor taskExecutor) {
        this.taskExecutor = Objects.requireNonNull(taskExecutor, "taskExecutor must not be null");
        this.underlying = taskExecutor.getThreadPoolExecutor();
    }

    // ---------------------------------------------------------------------------------------------
    // task submission — routed through TPTE so the TaskDecorator runs
    // ---------------------------------------------------------------------------------------------

    @Override
    public void execute(Runnable command) {
        taskExecutor.execute(command);
    }

    @Override
    public Future<?> submit(Runnable task) {
        return taskExecutor.submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        // TPTE only has submit(Runnable) / submit(Callable); adapt with
        // java.util.concurrent.Executors.
        return taskExecutor.submit(java.util.concurrent.Executors.callable(task, result));
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return taskExecutor.submit(task);
    }

    // ---------------------------------------------------------------------------------------------
    // lifecycle — delegate to the underlying pool
    // ---------------------------------------------------------------------------------------------

    @Override
    public void shutdown() {
        underlying.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return underlying.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return underlying.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return underlying.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return underlying.awaitTermination(timeout, unit);
    }

    // ---------------------------------------------------------------------------------------------
    // invokeAll / invokeAny — not used by the outbox engine, but implemented for completeness.
    // Delegation to the underlying pool means decoration is NOT applied; the engine doesn't
    // rely on these code paths.
    // ---------------------------------------------------------------------------------------------

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
            throws InterruptedException {
        return underlying.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(
            Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException {
        return underlying.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        return underlying.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, java.util.concurrent.ExecutionException, TimeoutException {
        return underlying.invokeAny(tasks, timeout, unit);
    }
}
