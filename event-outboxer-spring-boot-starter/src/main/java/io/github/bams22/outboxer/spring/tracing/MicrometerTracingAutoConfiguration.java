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

import io.github.bams22.outboxer.spi.OutboxTracer;
import io.github.bams22.outboxer.spring.OutboxProperties;
import io.github.bams22.outboxer.tracing.micrometer.MicrometerOutboxTracer;
import io.github.bams22.outboxer.tracing.micrometer.OutboxReceiverTracingObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.annotation.Order;

/**
 * Registers a {@link MicrometerOutboxTracer} when the {@code event-outboxer-tracing-micrometer}
 * adapter is on the classpath and Spring Boot's tracing auto-configuration provides {@link Tracer}
 * and {@link Propagator} beans (ADR-0023). The stored carrier then follows the application's {@code
 * management.tracing.propagation.*} and baggage settings.
 *
 * <p>The adapter instruments through the {@link ObservationRegistry}, but the conditions still
 * require {@link Tracer} and {@link Propagator}: those are exactly the beans under which Boot
 * registers the propagating tracing {@code ObservationHandler}s the adapter relies on. Without them
 * the registry would produce timers and no spans, and the OTel adapter is the better fallback.
 *
 * <p>Because those observations also feed meters, the observation names are prefixed with {@code
 * event-outboxer.metrics.prefix} — the same slot {@code MicrometerOutboxListener} uses, so both
 * modules publish into one namespace.
 *
 * <p>Takes precedence over {@code OtelTracingAutoConfiguration} when both tracing adapters are
 * present: Micrometer Tracing is Boot's first-class tracing abstraction, so the outbox carrier
 * matches every other outbound carrier the application emits. A user-defined {@link OutboxTracer}
 * bean beats both.
 */
@AutoConfiguration(
        afterName = {
            "org.springframework.boot.actuate.autoconfigure.observation.ObservationAutoConfiguration",
            "org.springframework.boot.actuate.autoconfigure.tracing.MicrometerTracingAutoConfiguration"
        })
@ConditionalOnClass({Tracer.class, MicrometerOutboxTracer.class})
@ConditionalOnBean({Tracer.class, Propagator.class, ObservationRegistry.class})
@ConditionalOnProperty(
        prefix = "event-outboxer.tracing",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true)
@EnableConfigurationProperties(OutboxProperties.class)
public class MicrometerTracingAutoConfiguration {

    /**
     * Bean order of {@link #outboxReceiverTracingObservationHandler}. Boot groups every {@code
     * TracingObservationHandler} bean into one first-matching composite in bean order, and its own
     * receiver handler sits at {@code RECEIVER_TRACING_OBSERVATION_HANDLER_ORDER} (1000). Ours only
     * claims the adapter's {@code OutboxReceiverContext} but must be asked first, or the generic
     * handler wins and deferred events silently keep the parent-child span shape.
     */
    public static final int RECEIVER_HANDLER_ORDER = 900;

    @Bean
    @ConditionalOnMissingBean(OutboxTracer.class)
    public OutboxTracer outboxMicrometerTracer(
            ObservationRegistry observationRegistry, Tracer tracer, OutboxProperties properties) {
        return new MicrometerOutboxTracer(
                observationRegistry, tracer, properties.getMetrics().getPrefix());
    }

    /**
     * Handler that gives deferred events their root-plus-link consumer span (ADR-0023, 2026-08-28
     * amendment). Registered unconditionally alongside the adapter so that a user-defined {@code
     * OutboxTracer} built on {@code MicrometerOutboxTracer} gets the same behaviour.
     */
    @Bean
    @Order(RECEIVER_HANDLER_ORDER)
    @ConditionalOnMissingBean(OutboxReceiverTracingObservationHandler.class)
    public OutboxReceiverTracingObservationHandler outboxReceiverTracingObservationHandler(
            Tracer tracer, Propagator propagator) {
        return new OutboxReceiverTracingObservationHandler(tracer, propagator);
    }
}
