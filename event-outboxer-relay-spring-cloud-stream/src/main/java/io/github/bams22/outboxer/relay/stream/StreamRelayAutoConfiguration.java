/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.relay.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.bams22.outboxer.api.publish.OutboxEventPublisher;
import io.github.bams22.outboxer.serializer.jackson.JacksonObjectMapperFactory;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;

/**
 * Self-wiring auto-configuration of the Spring Cloud Stream relay (ADR-0032) — the module ships its
 * own {@code AutoConfiguration.imports}, the starter knows nothing about it (the admin-modules
 * pattern). Active when {@code StreamBridge} is on the classpath; opt out with {@code
 * event-outboxer.relay.stream.enabled=false}.
 *
 * <p>The relay handler is picked up by the engine like any user {@code EventHandler} bean. Note
 * that defining an independent {@code EventHandler<StreamEnvelope>} bean for the same event type
 * (rather than overriding the {@code StreamRelayEventHandler} bean) fails startup with a duplicate
 * handler error. The facade bean backs off silently when no {@code OutboxEventPublisher} exists —
 * i.e. when the engine is disabled.
 */
@AutoConfiguration(
        afterName = {
            "io.github.bams22.outboxer.spring.OutboxEngineAutoConfiguration",
            "org.springframework.cloud.stream.function.FunctionConfiguration"
        })
@ConditionalOnClass(StreamBridge.class)
@ConditionalOnProperty(
        prefix = "event-outboxer.relay.stream",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true)
@EnableConfigurationProperties(StreamRelayProperties.class)
public class StreamRelayAutoConfiguration {

    /**
     * Default payload encoder. The {@link ObjectMapper} is resolved through the same chain as the
     * Jackson event serializer: the {@code outboxObjectMapper} bean, then Boot's primary mapper,
     * then {@code JacksonObjectMapperFactory.defaults()}.
     */
    @Bean
    @ConditionalOnMissingBean(StreamPayloadEncoder.class)
    public StreamPayloadEncoder outboxStreamPayloadEncoder(
            @Autowired(required = false) @Qualifier("outboxObjectMapper")
                    @Nullable ObjectMapper qualified,
            ObjectProvider<ObjectMapper> primary) {
        ObjectMapper mapper =
                qualified != null
                        ? qualified
                        : primary.getIfAvailable(JacksonObjectMapperFactory::defaults);
        return new JacksonStreamPayloadEncoder(mapper);
    }

    @Bean
    @ConditionalOnMissingBean(StreamRelayEventHandler.class)
    @ConditionalOnBean(StreamBridge.class)
    public StreamRelayEventHandler outboxStreamRelayEventHandler(
            StreamBridge streamBridge, StreamRelayProperties properties) {
        return StreamRelayEventHandler.builder()
                .streamOperations(streamBridge)
                .messageKeyHeader(properties.getMessageKeyHeader())
                .perKeyOrdering(properties.isPerKeyOrdering())
                .build();
    }

    @Bean
    @ConditionalOnMissingBean(StreamOutboxPublisher.class)
    @ConditionalOnBean(OutboxEventPublisher.class)
    public StreamOutboxPublisher outboxStreamOutboxPublisher(
            OutboxEventPublisher outboxEventPublisher,
            StreamPayloadEncoder encoder,
            StreamRelayProperties properties) {
        return DefaultStreamOutboxPublisher.builder()
                .outboxEventPublisher(outboxEventPublisher)
                .encoder(encoder)
                .defaultContentType(properties.getDefaultContentType())
                .build();
    }
}
