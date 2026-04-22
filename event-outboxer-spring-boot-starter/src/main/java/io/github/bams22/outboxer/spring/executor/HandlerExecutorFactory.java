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

import io.github.bams22.outboxer.core.config.EventTypeConfig;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;
import org.springframework.core.task.TaskDecorator;
import org.springframework.core.task.support.ContextPropagatingTaskDecorator;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Produces per-event-type handler executors for the Spring Boot starter. Two flavours,
 * selectable via {@code outbox.handler-executor.type}:
 *
 * <ul>
 *   <li>{@code platform} — Spring {@link ThreadPoolTaskExecutor} configured with a
 *       {@link ContextPropagatingTaskDecorator} so MDC, Micrometer Observation and security
 *       context captured on the submitting (poller) thread carry over to the handler thread.
 *       Exposed to the core engine as an {@code ExecutorService} via
 *       {@link SpringTaskExecutorAdapter} (the adapter is the key — submitting directly to
 *       the underlying pool would bypass decoration). Matches ADR-0009.
 *   <li>{@code virtual} — {@link Executors#newThreadPerTaskExecutor(java.util.concurrent.ThreadFactory)}
 *       backed by virtual threads, wrapped in {@link ContextPropagatingExecutorService} to
 *       apply the same {@link ContextPropagatingTaskDecorator}. Safe on JDK 25 thanks to
 *       JEP 491 (no pinning on {@code synchronized}).
 * </ul>
 */
public final class HandlerExecutorFactory {

  private HandlerExecutorFactory() {}

  /**
   * Fixed-size {@link ThreadPoolTaskExecutor} per event type.
   *
   * <p>Configuration:
   *
   * <ul>
   *   <li>{@code core = max = handlerPoolSize} — fixed pool; no on-demand scaling.
   *   <li>{@code queueCapacity = handlerQueueCapacity} — bounded LinkedBlockingQueue when
   *       positive; SynchronousQueue when 0 (tasks fail fast when the pool is saturated and
   *       surface as {@code onDispatchRejected}).
   *   <li>{@code keepAliveSeconds = 0} — core threads stay alive forever (no pool shrink).
   *   <li>Daemon threads named {@code outbox-handler-<N>}.
   *   <li>Rejection policy: {@link ThreadPoolExecutor.AbortPolicy}.
   *   <li>{@link ContextPropagatingTaskDecorator} for MDC / Observation / security context
   *       propagation from the poller thread.
   * </ul>
   */
  public static Function<EventTypeConfig, ExecutorService> platform() {
    return cfg -> {
      ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
      executor.setCorePoolSize(cfg.handlerPoolSize());
      executor.setMaxPoolSize(cfg.handlerPoolSize());
      executor.setQueueCapacity(cfg.handlerQueueCapacity());
      executor.setKeepAliveSeconds(0);
      executor.setAllowCoreThreadTimeOut(false);
      executor.setThreadNamePrefix("outbox-handler-");
      executor.setDaemon(true);
      executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
      executor.setTaskDecorator(contextPropagatingDecorator());
      executor.initialize();
      return new SpringTaskExecutorAdapter(executor);
    };
  }

  /** Virtual-thread factory; JEP 491 makes synchronized-heavy drivers safe on JDK 25. */
  public static Function<EventTypeConfig, ExecutorService> virtual() {
    TaskDecorator decorator = contextPropagatingDecorator();
    return cfg ->
        new ContextPropagatingExecutorService(
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("outbox-vt-", 0L).factory()),
            decorator);
  }

  /**
   * Factory for {@link ContextPropagatingTaskDecorator}. Extracted so tests can swap it out; in
   * production we always want Spring Framework's default, which reads all registered
   * {@code ThreadLocalAccessor}s via {@code ContextSnapshotFactory}.
   */
  private static TaskDecorator contextPropagatingDecorator() {
    return new ContextPropagatingTaskDecorator();
  }
}
