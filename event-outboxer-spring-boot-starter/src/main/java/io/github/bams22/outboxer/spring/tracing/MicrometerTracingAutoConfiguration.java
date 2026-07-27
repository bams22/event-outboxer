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
import io.github.bams22.outboxer.tracing.micrometer.MicrometerOutboxTracer;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * Registers a {@link MicrometerOutboxTracer} when the {@code event-outboxer-tracing-micrometer}
 * adapter is on the classpath and Spring Boot's tracing auto-configuration provides {@link Tracer}
 * and {@link Propagator} beans (ADR-0023). The stored carrier then follows the application's {@code
 * management.tracing.propagation.*} and baggage settings.
 *
 * <p>Takes precedence over {@code OtelTracingAutoConfiguration} when both tracing adapters are
 * present: Micrometer Tracing is Boot's first-class tracing abstraction, so the outbox carrier
 * matches every other outbound carrier the application emits. A user-defined {@link OutboxTracer}
 * bean beats both.
 */
@AutoConfiguration(
    afterName =
        "org.springframework.boot.actuate.autoconfigure.tracing.MicrometerTracingAutoConfiguration")
@ConditionalOnClass({Tracer.class, MicrometerOutboxTracer.class})
@ConditionalOnBean({Tracer.class, Propagator.class})
@ConditionalOnProperty(
    prefix = "event-outboxer.tracing",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
public class MicrometerTracingAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(OutboxTracer.class)
  public OutboxTracer outboxMicrometerTracer(Tracer tracer, Propagator propagator) {
    return new MicrometerOutboxTracer(tracer, propagator);
  }
}
