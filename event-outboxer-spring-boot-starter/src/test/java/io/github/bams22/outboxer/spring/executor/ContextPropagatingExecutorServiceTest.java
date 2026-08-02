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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.core.task.TaskDecorator;

class ContextPropagatingExecutorServiceTest {

    @Test
    void mdcPropagatesToTaskThread() throws Exception {
        ExecutorService delegate =
                Executors.newThreadPerTaskExecutor(
                        Thread.ofVirtual().name("test-vt-", 0L).factory());
        ContextPropagatingExecutorService exec =
                new ContextPropagatingExecutorService(delegate, new CapturingMdcTaskDecorator());

        AtomicReference<String> captured = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        MDC.put("traceId", "abc-123");
        try {
            exec.execute(
                    () -> {
                        captured.set(MDC.get("traceId"));
                        latch.countDown();
                    });
            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            MDC.clear();
        }

        assertThat(captured.get()).isEqualTo("abc-123");
        exec.shutdown();
        assertThat(exec.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void submitRunnableDecoratesTask() throws Exception {
        ExecutorService delegate = Executors.newSingleThreadExecutor();
        ContextPropagatingExecutorService exec =
                new ContextPropagatingExecutorService(delegate, new CapturingMdcTaskDecorator());

        AtomicReference<String> captured = new AtomicReference<>();
        MDC.put("k", "v");
        try {
            exec.submit(() -> captured.set(MDC.get("k"))).get(5, TimeUnit.SECONDS);
        } finally {
            MDC.clear();
        }

        assertThat(captured.get()).isEqualTo("v");
        exec.shutdown();
    }

    @Test
    void submitCallableDecoratesTaskAndPropagatesResult() throws Exception {
        ExecutorService delegate = Executors.newSingleThreadExecutor();
        ContextPropagatingExecutorService exec =
                new ContextPropagatingExecutorService(delegate, new CapturingMdcTaskDecorator());

        MDC.put("k", "v");
        try {
            Integer result =
                    exec.submit(() -> Integer.parseInt(MDC.get("k") == null ? "0" : "42"))
                            .get(5, TimeUnit.SECONDS);
            assertThat(result).isEqualTo(42);
        } finally {
            MDC.clear();
        }

        exec.shutdown();
    }

    @Test
    void shutdownDelegatesToWrappedExecutor() throws InterruptedException {
        ExecutorService delegate = Executors.newSingleThreadExecutor();
        ContextPropagatingExecutorService exec =
                new ContextPropagatingExecutorService(delegate, r -> r);

        assertThat(exec.isShutdown()).isFalse();
        exec.shutdown();
        assertThat(exec.isShutdown()).isTrue();
        assertThat(exec.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        assertThat(exec.isTerminated()).isTrue();
    }

    /**
     * Minimal stand-in for {@code ContextPropagatingTaskDecorator}: captures MDC at decoration time
     * and restores it inside the decorated Runnable. Doesn't pull in the micrometer
     * context-propagation lib, so the test suite stays self-contained.
     */
    private static final class CapturingMdcTaskDecorator implements TaskDecorator {
        @Override
        public Runnable decorate(Runnable task) {
            var snapshot = MDC.getCopyOfContextMap();
            return () -> {
                var previous = MDC.getCopyOfContextMap();
                if (snapshot != null) {
                    MDC.setContextMap(snapshot);
                } else {
                    MDC.clear();
                }
                try {
                    task.run();
                } finally {
                    if (previous != null) {
                        MDC.setContextMap(previous);
                    } else {
                        MDC.clear();
                    }
                }
            };
        }
    }
}
