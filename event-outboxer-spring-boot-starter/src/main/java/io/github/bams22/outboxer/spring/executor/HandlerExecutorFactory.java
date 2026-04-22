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
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;
import org.springframework.core.task.TaskDecorator;
import org.springframework.core.task.support.ContextPropagatingTaskDecorator;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Produces per-event-type handler executors for the Spring Boot starter. Two flavours,
 * selectable via {@code event-outboxer.handler-executor.type}:
 *
 * <ul>
 *   <li>{@code platform} — Spring {@link ThreadPoolTaskExecutor} configured with a
 *       {@link TaskDecorator} so MDC, Micrometer Observation and security context captured
 *       on the submitting (poller) thread carry over to the handler thread. Exposed to the
 *       core engine as an {@code ExecutorService} via {@link SpringTaskExecutorAdapter} (the
 *       adapter is the key — submitting directly to the underlying pool would bypass
 *       decoration). Matches ADR-0009. Works on every supported JDK.
 *   <li>{@code virtual} — virtual-thread-per-task {@code ExecutorService}, wrapped in
 *       {@link ContextPropagatingExecutorService} to apply the same {@link TaskDecorator}.
 *       <strong>Requires JDK 21+ at runtime</strong> (ADR-0017): the baseline is Java 17,
 *       so the virtual-thread APIs are invoked via reflection; on JDK < 21 the factory
 *       fails fast with a clear error when this flavour is selected. JDK 25+ additionally
 *       eliminates {@code synchronized} pinning via JEP 491.
 * </ul>
 *
 * <p>Both factory methods accept a {@link TaskDecorator}. The auto-configuration resolves
 * a user-defined {@code @Bean TaskDecorator} from the context when present and falls back
 * to {@link ContextPropagatingTaskDecorator} otherwise. The no-arg overloads use the same
 * default for programmatic (non-Spring) use.
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
   *   <li>The given {@link TaskDecorator} wraps every submission for context propagation
   *       from the poller thread.
   * </ul>
   */
  public static Function<EventTypeConfig, ExecutorService> platform(TaskDecorator decorator) {
    Objects.requireNonNull(decorator, "decorator must not be null");
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
      executor.setTaskDecorator(decorator);
      executor.initialize();
      return new SpringTaskExecutorAdapter(executor);
    };
  }

  /** {@code platform(...)} with the default {@link ContextPropagatingTaskDecorator}. */
  public static Function<EventTypeConfig, ExecutorService> platform() {
    return platform(new ContextPropagatingTaskDecorator());
  }

  /**
   * Virtual-thread factory; requires JDK 21+ at runtime (baseline Java 17 — APIs invoked via
   * reflection). JEP 491 makes {@code synchronized}-heavy drivers safe on JDK 25+.
   */
  public static Function<EventTypeConfig, ExecutorService> virtual(TaskDecorator decorator) {
    Objects.requireNonNull(decorator, "decorator must not be null");
    return cfg ->
        new ContextPropagatingExecutorService(newVirtualThreadPerTaskExecutor(), decorator);
  }

  /** {@code virtual(...)} with the default {@link ContextPropagatingTaskDecorator}. */
  public static Function<EventTypeConfig, ExecutorService> virtual() {
    return virtual(new ContextPropagatingTaskDecorator());
  }

  /**
   * Builds {@code Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("outbox-vt-",0L).factory())}
   * via reflection so the code compiles on a Java 17 baseline and binds late to JDK 21+ APIs
   * at runtime. Called once per event type at bean creation; reflection overhead is negligible
   * (not on the hot path). On JDK < 21 the missing {@code Thread.ofVirtual()} surfaces as an
   * actionable {@link IllegalStateException}.
   */
  private static ExecutorService newVirtualThreadPerTaskExecutor() {
    try {
      Class<?> builderCls = Class.forName("java.lang.Thread$Builder$OfVirtual");
      Method ofVirtual = Thread.class.getMethod("ofVirtual");
      Method name = builderCls.getMethod("name", String.class, long.class);
      Method factory = builderCls.getMethod("factory");
      Method newThreadPerTask =
          java.util.concurrent.Executors.class.getMethod(
              "newThreadPerTaskExecutor", ThreadFactory.class);

      Object builder = ofVirtual.invoke(null);
      builder = name.invoke(builder, "outbox-vt-", 0L);
      ThreadFactory tf = (ThreadFactory) factory.invoke(builder);
      return (ExecutorService) newThreadPerTask.invoke(null, tf);
    } catch (NoSuchMethodException | ClassNotFoundException ex) {
      throw new IllegalStateException(
          "virtual-thread handler executor requires JDK 21+ at runtime; current JVM is "
              + Runtime.version()
              + ". Use 'event-outboxer.handler-executor.type=platform' (default) or upgrade"
              + " the runtime. For JEP 491 (no synchronized pinning), use JDK 25+.",
          ex);
    } catch (ReflectiveOperationException ex) {
      throw new IllegalStateException(
          "failed to invoke Thread.ofVirtual() via reflection — unexpected for JDK "
              + Runtime.version(),
          ex);
    }
  }
}
