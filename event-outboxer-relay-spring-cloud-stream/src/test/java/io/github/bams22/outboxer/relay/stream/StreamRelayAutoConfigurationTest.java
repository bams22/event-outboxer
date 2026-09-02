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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.publish.OutboxEventPublisher;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.WorkerId;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;

class StreamRelayAutoConfigurationTest {

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner()
                    .withConfiguration(AutoConfigurations.of(StreamRelayAutoConfiguration.class));

    private final StreamBridge streamBridge = mock(StreamBridge.class);

    private ApplicationContextRunner withCollaborators() {
        return runner.withBean(StreamBridge.class, () -> streamBridge)
                .withBean(OutboxEventPublisher.class, () -> mock(OutboxEventPublisher.class));
    }

    private record Payload(String orderId) {}

    private static EventContext context() {
        return EventContext.builder()
                .eventId(UUID.randomUUID())
                .eventType(StreamEnvelope.EVENT_TYPE_NAME)
                .attempt(1)
                .createdAt(Instant.now())
                .claimedAt(Instant.now())
                .workerId(WorkerId.generateDefault())
                .traceContext(Map.of())
                .build();
    }

    private static StreamEnvelope envelope() {
        return StreamEnvelope.builder()
                .binding("b")
                .key("k")
                .contentType("application/json")
                .textPayload("{}")
                .build();
    }

    /**
     * Drives the auto-configured handler through one delivery and returns the message it handed to
     * StreamBridge — the only way to observe that a relay property actually reached the bean rather
     * than merely binding into StreamRelayProperties.
     */
    private Message<?> deliverAndCapture(StreamRelayEventHandler handler) {
        when(streamBridge.send(any(String.class), any())).thenReturn(true);
        handler.handle(context(), envelope());
        ArgumentCaptor<Message<?>> captor = ArgumentCaptor.forClass(Message.class);
        verify(streamBridge).send(eq("b"), captor.capture());
        return captor.getValue();
    }

    @Test
    void allBeansPresentByDefault() {
        withCollaborators()
                .run(
                        ctx -> {
                            assertThat(ctx).hasSingleBean(StreamPayloadEncoder.class);
                            assertThat(ctx).hasSingleBean(StreamRelayEventHandler.class);
                            assertThat(ctx).hasSingleBean(StreamOutboxPublisher.class);
                        });
    }

    @Test
    void perKeyOrderingPropertyReachesTheHandler() {
        withCollaborators()
                .withPropertyValues("event-outboxer.relay.stream.per-key-ordering=true")
                .run(
                        ctx ->
                                assertThat(
                                                ctx.getBean(StreamRelayEventHandler.class)
                                                        .extractLockKey(envelope()))
                                        .isEqualTo("outboxer-stream-relay:b:k"));
    }

    @Test
    void killSwitchDisablesEverything() {
        withCollaborators()
                .withPropertyValues("event-outboxer.relay.stream.enabled=false")
                .run(
                        ctx -> {
                            assertThat(ctx).doesNotHaveBean(StreamPayloadEncoder.class);
                            assertThat(ctx).doesNotHaveBean(StreamRelayEventHandler.class);
                            assertThat(ctx).doesNotHaveBean(StreamOutboxPublisher.class);
                        });
    }

    @Test
    void backsOffWithoutStreamBridgeOnClasspath() {
        runner.withClassLoader(new FilteredClassLoader(StreamBridge.class))
                .withBean(OutboxEventPublisher.class, () -> mock(OutboxEventPublisher.class))
                .run(
                        ctx -> {
                            assertThat(ctx).doesNotHaveBean(StreamPayloadEncoder.class);
                            assertThat(ctx).doesNotHaveBean(StreamOutboxPublisher.class);
                        });
    }

    @Test
    void noStreamBridgeBeanMeansNoHandler() {
        runner.withBean(OutboxEventPublisher.class, () -> mock(OutboxEventPublisher.class))
                .run(
                        ctx -> {
                            assertThat(ctx).doesNotHaveBean(StreamRelayEventHandler.class);
                            assertThat(ctx).hasSingleBean(StreamOutboxPublisher.class);
                        });
    }

    @Test
    void noOutboxEventPublisherBeanMeansNoFacade() {
        runner.withBean(StreamBridge.class, () -> mock(StreamBridge.class))
                .run(
                        ctx -> {
                            assertThat(ctx).doesNotHaveBean(StreamOutboxPublisher.class);
                            assertThat(ctx).hasSingleBean(StreamRelayEventHandler.class);
                        });
    }

    @Test
    void userDefinedBeansWin() {
        StreamPayloadEncoder customEncoder = payload -> SerializedPayload.ofText("custom");
        StreamOutboxPublisher customPublisher = mock(StreamOutboxPublisher.class);

        withCollaborators()
                .withBean("myEncoder", StreamPayloadEncoder.class, () -> customEncoder)
                .withBean("myPublisher", StreamOutboxPublisher.class, () -> customPublisher)
                .run(
                        ctx -> {
                            assertThat(ctx.getBean(StreamPayloadEncoder.class))
                                    .isSameAs(customEncoder);
                            assertThat(ctx.getBean(StreamOutboxPublisher.class))
                                    .isSameAs(customPublisher);
                        });
    }

    @Test
    void outboxObjectMapperQualifiedBeanIsPreferredOverPrimary() {
        ObjectMapper upperCamel =
                new ObjectMapper()
                        .setPropertyNamingStrategy(PropertyNamingStrategies.UPPER_CAMEL_CASE);

        withCollaborators()
                .withBean("plainMapper", ObjectMapper.class, ObjectMapper::new)
                .withBean("outboxObjectMapper", ObjectMapper.class, () -> upperCamel)
                .run(
                        ctx -> {
                            String wire =
                                    ctx.getBean(StreamPayloadEncoder.class)
                                            .encode(new Payload("o-1"))
                                            .requireText();
                            assertThat(wire).contains("\"OrderId\"");
                        });
    }

    @Test
    void primaryObjectMapperIsUsedWhenNoQualifiedBeanExists() {
        ObjectMapper upperCamel =
                new ObjectMapper()
                        .setPropertyNamingStrategy(PropertyNamingStrategies.UPPER_CAMEL_CASE);

        withCollaborators()
                .withBean(ObjectMapper.class, () -> upperCamel)
                .run(
                        ctx -> {
                            String wire =
                                    ctx.getBean(StreamPayloadEncoder.class)
                                            .encode(new Payload("o-1"))
                                            .requireText();
                            assertThat(wire).contains("\"OrderId\"");
                        });
    }

    @Test
    void customKeyHeaderPropertyReachesTheHandler() {
        withCollaborators()
                .withPropertyValues("event-outboxer.relay.stream.message-key-header=myKey")
                .run(
                        ctx -> {
                            Message<?> message =
                                    deliverAndCapture(ctx.getBean(StreamRelayEventHandler.class));

                            assertThat((byte[]) message.getHeaders().get("myKey"))
                                    .isEqualTo("k".getBytes(StandardCharsets.UTF_8));
                            assertThat(message.getHeaders())
                                    .doesNotContainKey(
                                            StreamRelayEventHandler.DEFAULT_MESSAGE_KEY_HEADER);
                        });
    }

    @Test
    void blankKeyHeaderPropertyReachesTheHandler() {
        withCollaborators()
                .withPropertyValues("event-outboxer.relay.stream.message-key-header=")
                .run(
                        ctx -> {
                            Message<?> message =
                                    deliverAndCapture(ctx.getBean(StreamRelayEventHandler.class));

                            assertThat(message.getHeaders())
                                    .doesNotContainKey(
                                            StreamRelayEventHandler.DEFAULT_MESSAGE_KEY_HEADER);
                            assertThat(ctx.getBean(StreamRelayProperties.class))
                                    .extracting(StreamRelayProperties::getMessageKeyHeader)
                                    .isEqualTo("");
                        });
    }

    @Test
    void defaultContentTypePropertyBinds() {
        withCollaborators()
                .withPropertyValues(
                        "event-outboxer.relay.stream.default-content-type=application/xml")
                .run(
                        ctx ->
                                assertThat(
                                                ctx.getBean(StreamRelayProperties.class)
                                                        .getDefaultContentType())
                                        .isEqualTo("application/xml"));
    }
}
