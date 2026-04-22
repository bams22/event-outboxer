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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.core.task.TaskDecorator;

/**
 * Wraps an {@link ExecutorService} and applies a {@link TaskDecorator} to every submitted
 * {@link Runnable} / {@link Callable}. Used for the virtual-thread executor where Spring's
 * {@code ThreadPoolTaskExecutor} doesn't apply: the underlying
 * {@code Executors.newThreadPerTaskExecutor(...)} is a plain {@code ExecutorService}, and we
 * still want MDC / Micrometer Observation / security context to propagate from the submitting
 * thread to the virtual thread running the task.
 */
final class ContextPropagatingExecutorService implements ExecutorService {

  private final ExecutorService delegate;
  private final TaskDecorator decorator;

  ContextPropagatingExecutorService(ExecutorService delegate, TaskDecorator decorator) {
    this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
    this.decorator = Objects.requireNonNull(decorator, "decorator must not be null");
  }

  @Override
  public void execute(Runnable command) {
    delegate.execute(decorator.decorate(command));
  }

  @Override
  public Future<?> submit(Runnable task) {
    return delegate.submit(decorator.decorate(task));
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return delegate.submit(decorator.decorate(task), result);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return delegate.submit(decorateCallable(task));
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    return delegate.invokeAll(tasks.stream().map(this::decorateCallable).toList());
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException {
    return delegate.invokeAll(tasks.stream().map(this::decorateCallable).toList(), timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, java.util.concurrent.ExecutionException {
    return delegate.invokeAny(tasks.stream().map(this::decorateCallable).toList());
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, java.util.concurrent.ExecutionException, TimeoutException {
    return delegate.invokeAny(tasks.stream().map(this::decorateCallable).toList(), timeout, unit);
  }

  @Override
  public void shutdown() {
    delegate.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return delegate.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return delegate.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return delegate.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return delegate.awaitTermination(timeout, unit);
  }

  // TaskDecorator operates on Runnable; for Callable we wrap the call in a Runnable,
  // decorate at SUBMIT TIME (on the submitting thread, so the decorator can capture its MDC
  // / Observation / security context), then unwrap the result via a holder on the worker
  // thread. Preserves exceptions and results.
  private <T> Callable<T> decorateCallable(Callable<T> task) {
    Object[] box = new Object[2]; // [result, exception]
    Runnable work =
        () -> {
          try {
            box[0] = task.call();
          } catch (Exception ex) {
            box[1] = ex;
          }
        };
    Runnable decorated = decorator.decorate(work);
    return () -> {
      decorated.run();
      if (box[1] != null) {
        throw (Exception) box[1];
      }
      @SuppressWarnings("unchecked")
      T result = (T) box[0];
      return result;
    };
  }
}
