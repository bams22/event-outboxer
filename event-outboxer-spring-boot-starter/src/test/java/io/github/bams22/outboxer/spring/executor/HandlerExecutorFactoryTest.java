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

import io.github.bams22.outboxer.core.config.EventTypeConfig;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Covers the {@code virtual} flavour of {@link HandlerExecutorFactory}: tasks must run on named
 * virtual threads and pass through the configured {@code TaskDecorator}.
 */
class HandlerExecutorFactoryTest {

    @Test
    void virtualRunsTasksOnVirtualThreads() throws Exception {
        ExecutorService exec = HandlerExecutorFactory.virtual().apply(EventTypeConfig.defaults());
        try {
            AtomicBoolean virtual = new AtomicBoolean();
            AtomicReference<String> threadName = new AtomicReference<>();
            exec.submit(
                            () -> {
                                virtual.set(Thread.currentThread().isVirtual());
                                threadName.set(Thread.currentThread().getName());
                            })
                    .get(5, TimeUnit.SECONDS);

            assertThat(virtual).isTrue();
            assertThat(threadName.get()).startsWith("outbox-vt-");
        } finally {
            exec.shutdown();
            assertThat(exec.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void virtualAppliesTaskDecorator() throws Exception {
        AtomicBoolean decorated = new AtomicBoolean();
        ExecutorService exec =
                HandlerExecutorFactory.virtual(
                                task ->
                                        () -> {
                                            decorated.set(true);
                                            task.run();
                                        })
                        .apply(EventTypeConfig.defaults());
        try {
            AtomicBoolean ran = new AtomicBoolean();
            exec.submit(() -> ran.set(true)).get(5, TimeUnit.SECONDS);

            assertThat(decorated).isTrue();
            assertThat(ran).isTrue();
        } finally {
            exec.shutdown();
            assertThat(exec.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }
}
