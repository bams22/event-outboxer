/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.github.bams22.outboxer.spi.OutboxTracer;
import io.github.bams22.outboxer.tracing.micrometer.MicrometerOutboxTracer;
import io.github.bams22.outboxer.tracing.otel.OtelOutboxTracer;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import io.opentelemetry.api.OpenTelemetry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Conditional-activation matrix of the two tracing auto-configurations (ADR-0023): classpath
 * detection, bean conditions, adapter precedence, the {@code event-outboxer.tracing.enabled}
 * switch, and user-bean override.
 */
class TracingAutoConfigurationTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(
              AutoConfigurations.of(
                  MicrometerTracingAutoConfiguration.class, OtelTracingAutoConfiguration.class));

  @Test
  void noTracingLibrariesOnClasspathMeansNoTracerBean() {
    runner
        .withClassLoader(new FilteredClassLoader(Tracer.class, OpenTelemetry.class))
        .run(context -> assertThat(context).doesNotHaveBean(OutboxTracer.class));
  }

  @Test
  void micrometerBeansActivateTheMicrometerAdapter() {
    runner
        .withBean(Tracer.class, () -> mock(Tracer.class))
        .withBean(Propagator.class, () -> mock(Propagator.class))
        .withClassLoader(new FilteredClassLoader(OpenTelemetry.class))
        .run(
            context ->
                assertThat(context)
                    .getBean(OutboxTracer.class)
                    .isInstanceOf(MicrometerOutboxTracer.class));
  }

  @Test
  void otelBeanActivatesTheOtelAdapterWhenMicrometerAbsent() {
    runner
        .withBean(OpenTelemetry.class, OpenTelemetry::noop)
        .withClassLoader(new FilteredClassLoader(Tracer.class))
        .run(
            context ->
                assertThat(context)
                    .getBean(OutboxTracer.class)
                    .isInstanceOf(OtelOutboxTracer.class));
  }

  @Test
  void otelAdapterFallsBackToGlobalOpenTelemetryWithoutABean() {
    runner
        .withClassLoader(new FilteredClassLoader(Tracer.class))
        .run(
            context ->
                assertThat(context)
                    .getBean(OutboxTracer.class)
                    .isInstanceOf(OtelOutboxTracer.class));
  }

  @Test
  void micrometerWinsWhenBothAdaptersAreAvailable() {
    runner
        .withBean(Tracer.class, () -> mock(Tracer.class))
        .withBean(Propagator.class, () -> mock(Propagator.class))
        .withBean(OpenTelemetry.class, OpenTelemetry::noop)
        .run(
            context -> {
              assertThat(context).hasSingleBean(OutboxTracer.class);
              assertThat(context)
                  .getBean(OutboxTracer.class)
                  .isInstanceOf(MicrometerOutboxTracer.class);
            });
  }

  @Test
  void micrometerWithoutPropagatorBeanBacksOffToOtel() {
    runner
        .withBean(Tracer.class, () -> mock(Tracer.class))
        .withBean(OpenTelemetry.class, OpenTelemetry::noop)
        .run(
            context ->
                assertThat(context)
                    .getBean(OutboxTracer.class)
                    .isInstanceOf(OtelOutboxTracer.class));
  }

  @Test
  void disabledPropertySuppressesBothAdapters() {
    runner
        .withPropertyValues("event-outboxer.tracing.enabled=false")
        .withBean(Tracer.class, () -> mock(Tracer.class))
        .withBean(Propagator.class, () -> mock(Propagator.class))
        .withBean(OpenTelemetry.class, OpenTelemetry::noop)
        .run(context -> assertThat(context).doesNotHaveBean(OutboxTracer.class));
  }

  @Test
  void userDefinedTracerBeanBeatsBothAdapters() {
    OutboxTracer custom = OutboxTracer.NOOP;
    runner
        .withBean("customOutboxTracer", OutboxTracer.class, () -> custom)
        .withBean(Tracer.class, () -> mock(Tracer.class))
        .withBean(Propagator.class, () -> mock(Propagator.class))
        .withBean(OpenTelemetry.class, OpenTelemetry::noop)
        .run(
            context -> {
              assertThat(context).hasSingleBean(OutboxTracer.class);
              assertThat(context.getBean(OutboxTracer.class)).isSameAs(custom);
            });
  }
}
