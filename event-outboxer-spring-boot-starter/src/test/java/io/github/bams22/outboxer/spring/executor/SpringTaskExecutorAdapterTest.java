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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

class SpringTaskExecutorAdapterTest {

  private ThreadPoolTaskExecutor taskExecutor;

  @BeforeEach
  void setUp() {
    taskExecutor = new ThreadPoolTaskExecutor();
    taskExecutor.setCorePoolSize(1);
    taskExecutor.setMaxPoolSize(1);
    taskExecutor.setQueueCapacity(10);
    taskExecutor.setThreadNamePrefix("test-adapter-");
  }

  @AfterEach
  void tearDown() {
    if (taskExecutor != null) {
      taskExecutor.shutdown();
    }
  }

  @Test
  void executeRoutesThroughTaskDecorator() throws Exception {
    AtomicBoolean decoratorCalled = new AtomicBoolean(false);
    taskExecutor.setTaskDecorator(
        runnable -> {
          decoratorCalled.set(true);
          return runnable;
        });
    taskExecutor.initialize();

    ExecutorService adapter = new SpringTaskExecutorAdapter(taskExecutor);
    CountDownLatch latch = new CountDownLatch(1);
    adapter.execute(latch::countDown);

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(decoratorCalled).isTrue();
  }

  @Test
  void submitRunnableRoutesThroughTaskDecorator() throws Exception {
    AtomicBoolean decoratorCalled = new AtomicBoolean(false);
    taskExecutor.setTaskDecorator(
        runnable -> {
          decoratorCalled.set(true);
          return runnable;
        });
    taskExecutor.initialize();

    ExecutorService adapter = new SpringTaskExecutorAdapter(taskExecutor);
    adapter.submit(() -> {}).get(5, TimeUnit.SECONDS);

    assertThat(decoratorCalled).isTrue();
  }

  @Test
  void submitCallableRoutesThroughTaskDecorator() throws Exception {
    AtomicBoolean decoratorCalled = new AtomicBoolean(false);
    taskExecutor.setTaskDecorator(
        runnable -> {
          decoratorCalled.set(true);
          return runnable;
        });
    taskExecutor.initialize();

    ExecutorService adapter = new SpringTaskExecutorAdapter(taskExecutor);
    Integer result = adapter.submit(() -> 42).get(5, TimeUnit.SECONDS);

    assertThat(result).isEqualTo(42);
    assertThat(decoratorCalled).isTrue();
  }

  @Test
  void decoratorSeesContextFromSubmittingThread() throws Exception {
    AtomicReference<String> captured = new AtomicReference<>();
    taskExecutor.setTaskDecorator(
        runnable -> {
          // Captured at submit time, stored in an AtomicReference, restored at run time.
          String threadLocalSnapshot = Thread.currentThread().getName();
          return () -> {
            captured.set(threadLocalSnapshot);
            runnable.run();
          };
        });
    taskExecutor.initialize();

    ExecutorService adapter = new SpringTaskExecutorAdapter(taskExecutor);
    CountDownLatch latch = new CountDownLatch(1);
    adapter.execute(latch::countDown);
    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

    // The decorator ran on the TEST thread (not inside the pool).
    assertThat(captured.get()).isEqualTo(Thread.currentThread().getName());
  }

  @Test
  void shutdownDelegatesToUnderlyingPool() {
    taskExecutor.initialize();
    ExecutorService adapter = new SpringTaskExecutorAdapter(taskExecutor);

    assertThat(adapter.isShutdown()).isFalse();
    adapter.shutdown();
    assertThat(adapter.isShutdown()).isTrue();
  }

  @Test
  void awaitTerminationReturnsTrueAfterShutdown() throws InterruptedException {
    taskExecutor.initialize();
    ExecutorService adapter = new SpringTaskExecutorAdapter(taskExecutor);

    adapter.shutdown();
    assertThat(adapter.awaitTermination(1, TimeUnit.SECONDS)).isTrue();
    assertThat(adapter.isTerminated()).isTrue();
  }
}
