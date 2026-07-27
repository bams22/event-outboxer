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
import io.github.bams22.outboxer.tracing.otel.OtelOutboxTracer;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * Registers an {@link OtelOutboxTracer} when the {@code event-outboxer-tracing-otel} adapter and
 * the OpenTelemetry API are on the classpath (ADR-0023). Prefers an {@link OpenTelemetry} bean
 * (present with Boot's OTLP/micrometer-tracing-bridge-otel setup); falls back to {@link
 * GlobalOpenTelemetry#get()} for applications instrumented by the OpenTelemetry Java agent,
 * where no bean exists. Without any SDK or agent the global is a functional no-op and the
 * adapter degrades to storing an empty trace context.
 *
 * <p>Runs after {@link MicrometerTracingAutoConfiguration} and backs off when that (or the user)
 * already registered an {@link OutboxTracer} bean.
 *
 * <p>Caveat on the global fallback: {@code GlobalOpenTelemetry.get()} pins the global instance.
 * Applications that call {@code GlobalOpenTelemetry.set(...)} <em>after</em> context refresh
 * must instead expose an {@link OpenTelemetry} bean or a custom {@link OutboxTracer} bean.
 */
@AutoConfiguration(
    after = MicrometerTracingAutoConfiguration.class,
    afterName =
        "org.springframework.boot.actuate.autoconfigure.opentelemetry.OpenTelemetryAutoConfiguration")
@ConditionalOnClass({OpenTelemetry.class, OtelOutboxTracer.class})
@ConditionalOnProperty(
    prefix = "event-outboxer.tracing",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
public class OtelTracingAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(OutboxTracer.class)
  public OutboxTracer outboxOtelTracer(ObjectProvider<OpenTelemetry> openTelemetryProvider) {
    return new OtelOutboxTracer(openTelemetryProvider.getIfAvailable(GlobalOpenTelemetry::get));
  }
}
