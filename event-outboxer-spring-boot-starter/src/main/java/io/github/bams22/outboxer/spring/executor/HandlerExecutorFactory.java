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

import io.github.bams22.outboxer.core.concurrent.NamedThreadFactory;
import io.github.bams22.outboxer.core.config.EventTypeConfig;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Produces per-event-type handler executors. Two flavours, selectable via {@code
 * outbox.handler-executor.type}:
 *
 * <ul>
 *   <li>{@code platform} — fixed-size {@link ThreadPoolExecutor} with a bounded queue (see
 *       {@link EventTypeConfig#handlerQueueCapacity()}). Rejections surface as {@code
 *       onDispatchRejected} and the event is eligible again after a short retry delay.
 *   <li>{@code virtual} — {@link Executors#newThreadPerTaskExecutor(java.util.concurrent.ThreadFactory)}
 *       backed by virtual threads. Safe on JDK 25 thanks to JEP 491 (no pinning on
 *       {@code synchronized}).
 * </ul>
 *
 * <p>Spring Boot's {@code ContextPropagatingTaskDecorator} for MDC / Micrometer Observation
 * propagation is deferred to a future refinement — platform handlers still carry the request
 * context because the engine reads {@code claimed.traceContext()} and restores it manually.
 */
public final class HandlerExecutorFactory {

  private HandlerExecutorFactory() {}

  /** Fixed-thread-pool factory matching the core builder default. */
  public static Function<EventTypeConfig, ExecutorService> platform() {
    return cfg ->
        new ThreadPoolExecutor(
            cfg.handlerPoolSize(),
            cfg.handlerPoolSize(),
            0L,
            TimeUnit.MILLISECONDS,
            queueOf(cfg),
            new NamedThreadFactory("outbox-handler", true),
            new ThreadPoolExecutor.AbortPolicy());
  }

  /** Virtual-thread factory; JEP 491 makes synchronized-heavy drivers safe on JDK 25. */
  public static Function<EventTypeConfig, ExecutorService> virtual() {
    return cfg ->
        Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("outbox-vt-", 0L).factory());
  }

  private static BlockingQueue<Runnable> queueOf(EventTypeConfig cfg) {
    int cap = cfg.handlerQueueCapacity();
    if (cap == 0) {
      return new SynchronousQueue<>();
    }
    return cap > 0 ? new ArrayBlockingQueue<>(cap) : new LinkedBlockingQueue<>();
  }
}
