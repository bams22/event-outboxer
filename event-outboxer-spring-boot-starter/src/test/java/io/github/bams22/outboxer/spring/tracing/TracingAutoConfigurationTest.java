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
import io.github.bams22.outboxer.spring.OutboxProperties;
import io.github.bams22.outboxer.tracing.micrometer.MicrometerOutboxTracer;
import io.github.bams22.outboxer.tracing.micrometer.OutboxReceiverTracingObservationHandler;
import io.github.bams22.outboxer.tracing.otel.OtelOutboxTracer;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import io.opentelemetry.api.OpenTelemetry;
import java.time.Duration;
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
                                    MicrometerTracingAutoConfiguration.class,
                                    OtelTracingAutoConfiguration.class));

    /**
     * The bean set Boot's tracing auto-configuration provides: the registry the adapter instruments
     * through, plus the two beans that gate the propagating tracing handlers.
     */
    private ApplicationContextRunner withMicrometerTracingBeans(ApplicationContextRunner runner) {
        return runner.withBean(Tracer.class, () -> mock(Tracer.class))
                .withBean(Propagator.class, () -> mock(Propagator.class))
                .withBean(ObservationRegistry.class, ObservationRegistry::create);
    }

    @Test
    void noTracingLibrariesOnClasspathMeansNoTracerBean() {
        runner.withClassLoader(new FilteredClassLoader(Tracer.class, OpenTelemetry.class))
                .run(context -> assertThat(context).doesNotHaveBean(OutboxTracer.class));
    }

    @Test
    void micrometerBeansActivateTheMicrometerAdapter() {
        withMicrometerTracingBeans(runner)
                .withClassLoader(new FilteredClassLoader(OpenTelemetry.class))
                .run(
                        context ->
                                assertThat(context)
                                        .getBean(OutboxTracer.class)
                                        .isInstanceOf(MicrometerOutboxTracer.class));
    }

    @Test
    void otelBeanActivatesTheOtelAdapterWhenMicrometerAbsent() {
        runner.withBean(OpenTelemetry.class, OpenTelemetry::noop)
                .withClassLoader(new FilteredClassLoader(Tracer.class))
                .run(
                        context ->
                                assertThat(context)
                                        .getBean(OutboxTracer.class)
                                        .isInstanceOf(OtelOutboxTracer.class));
    }

    @Test
    void otelAdapterFallsBackToGlobalOpenTelemetryWithoutABean() {
        runner.withClassLoader(new FilteredClassLoader(Tracer.class))
                .run(
                        context ->
                                assertThat(context)
                                        .getBean(OutboxTracer.class)
                                        .isInstanceOf(OtelOutboxTracer.class));
    }

    @Test
    void micrometerWinsWhenBothAdaptersAreAvailable() {
        withMicrometerTracingBeans(runner)
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
        runner.withBean(Tracer.class, () -> mock(Tracer.class))
                .withBean(ObservationRegistry.class, ObservationRegistry::create)
                .withBean(OpenTelemetry.class, OpenTelemetry::noop)
                .run(
                        context ->
                                assertThat(context)
                                        .getBean(OutboxTracer.class)
                                        .isInstanceOf(OtelOutboxTracer.class));
    }

    @Test
    void micrometerWithoutObservationRegistryBeanBacksOffToOtel() {
        runner.withBean(Tracer.class, () -> mock(Tracer.class))
                .withBean(Propagator.class, () -> mock(Propagator.class))
                .withBean(OpenTelemetry.class, OpenTelemetry::noop)
                .run(
                        context ->
                                assertThat(context)
                                        .getBean(OutboxTracer.class)
                                        .isInstanceOf(OtelOutboxTracer.class));
    }

    @Test
    void disabledPropertySuppressesBothAdapters() {
        withMicrometerTracingBeans(runner)
                .withPropertyValues("event-outboxer.tracing.enabled=false")
                .withBean(OpenTelemetry.class, OpenTelemetry::noop)
                .run(context -> assertThat(context).doesNotHaveBean(OutboxTracer.class));
    }

    /**
     * The receiver handler that gives deferred events their root-plus-link span (ADR-0023,
     * 2026-08-28 amendment) ships with the Micrometer adapter and only with it.
     */
    @Test
    void micrometerBeansAlsoRegisterTheOutboxReceiverHandler() {
        withMicrometerTracingBeans(runner)
                .withClassLoader(new FilteredClassLoader(OpenTelemetry.class))
                .run(
                        context ->
                                assertThat(context)
                                        .hasSingleBean(
                                                OutboxReceiverTracingObservationHandler.class));
    }

    @Test
    void otelOnlySetupRegistersNoReceiverHandler() {
        runner.withBean(OpenTelemetry.class, OpenTelemetry::noop)
                .run(
                        context ->
                                assertThat(context)
                                        .doesNotHaveBean(
                                                OutboxReceiverTracingObservationHandler.class));
    }

    @Test
    void userDefinedReceiverHandlerBeanWins() {
        withMicrometerTracingBeans(runner)
                .withBean(
                        "customReceiverHandler",
                        OutboxReceiverTracingObservationHandler.class,
                        () ->
                                new OutboxReceiverTracingObservationHandler(
                                        mock(Tracer.class), mock(Propagator.class)))
                .run(
                        context ->
                                assertThat(context)
                                        .hasSingleBean(
                                                OutboxReceiverTracingObservationHandler.class)
                                        .hasBean("customReceiverHandler"));
    }

    @Test
    void deferredEventsLinkBeyondOneMinuteByDefault() {
        withMicrometerTracingBeans(runner)
                .run(
                        context -> {
                            OutboxProperties.Tracing tracing =
                                    context.getBean(OutboxProperties.class).getTracing();
                            assertThat(tracing.getDeferredPropagation())
                                    .isEqualTo(OutboxTracer.Propagation.LINK);
                            assertThat(tracing.getLinkThreshold()).isEqualTo(Duration.ofMinutes(1));
                            assertThat(tracing.resolveLinkThreshold())
                                    .isEqualTo(Duration.ofMinutes(1));
                        });
    }

    @Test
    void linkThresholdBindsAndChildPropagationDisablesTheRule() {
        withMicrometerTracingBeans(runner)
                .withPropertyValues("event-outboxer.tracing.link-threshold=0s")
                .run(
                        context ->
                                assertThat(
                                                context.getBean(OutboxProperties.class)
                                                        .getTracing()
                                                        .resolveLinkThreshold())
                                        .isEqualTo(Duration.ZERO));
        withMicrometerTracingBeans(runner)
                .withPropertyValues("event-outboxer.tracing.deferred-propagation=child")
                .run(
                        context -> {
                            OutboxProperties.Tracing tracing =
                                    context.getBean(OutboxProperties.class).getTracing();
                            assertThat(tracing.getDeferredPropagation())
                                    .isEqualTo(OutboxTracer.Propagation.CHILD);
                            assertThat(tracing.resolveLinkThreshold()).isNull();
                        });
    }

    @Test
    void userDefinedTracerBeanBeatsBothAdapters() {
        OutboxTracer custom = OutboxTracer.NOOP;
        withMicrometerTracingBeans(runner)
                .withBean("customOutboxTracer", OutboxTracer.class, () -> custom)
                .withBean(OpenTelemetry.class, OpenTelemetry::noop)
                .run(
                        context -> {
                            assertThat(context).hasSingleBean(OutboxTracer.class);
                            assertThat(context.getBean(OutboxTracer.class)).isSameAs(custom);
                        });
    }
}
