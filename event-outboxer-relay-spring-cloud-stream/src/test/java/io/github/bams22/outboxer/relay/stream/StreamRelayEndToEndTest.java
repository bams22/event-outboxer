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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.spring.storage.OutboxInMemoryTestConfiguration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

/**
 * Full-stack test: facade publish → engine claims and dispatches → built-in handler delivers
 * through the Spring Cloud Stream test binder. Transaction-join semantics, retries and tracing are
 * deliberately NOT re-tested here — they are owned by the core and starter suites.
 */
@SpringBootTest(
        classes = StreamRelayEndToEndTest.TestApp.class,
        properties = {
            "event-outboxer.publisher.no-transaction-policy=IGNORE",
            "event-outboxer.event-types.defaults.poll-min-interval=20ms",
            "event-outboxer.event-types.defaults.poll-max-interval=50ms",
            "event-outboxer.event-types.defaults.handler-pool-size=2"
        })
@Import({OutboxInMemoryTestConfiguration.class, TestChannelBinderConfiguration.class})
class StreamRelayEndToEndTest {

    @Autowired StreamOutboxPublisher publisher;
    @Autowired OutputDestination outputDestination;

    record OrderCreated(String orderId, int count) {}

    @Test
    void publishedMessageIsRelayedToTheBinding() {
        publisher.publish(
                StreamOutboxMessage.builder()
                        .binding("orders-out")
                        .key("k1")
                        .headers(Map.of("x-app", "1"))
                        .payload(new OrderCreated("o-1", 3))
                        .build());

        Message<byte[]> received = outputDestination.receive(5000, "orders-out");

        assertThat(received).isNotNull();
        assertThat(new String(received.getPayload(), UTF_8))
                .contains("\"orderId\":\"o-1\"")
                .contains("\"count\":3");
        assertThat(received.getHeaders()).containsEntry("x-app", "1");
        assertThat((byte[]) received.getHeaders().get("kafka_messageKey"))
                .isEqualTo("k1".getBytes(UTF_8));
        assertThat(received.getHeaders().get(MessageHeaders.CONTENT_TYPE))
                .hasToString("application/json");
    }

    @Test
    void publishAllRelaysEveryMessage() {
        publisher.publishAll(
                List.of(
                        StreamOutboxMessage.of("batch-out", "a", new OrderCreated("o-a", 1)),
                        StreamOutboxMessage.of("batch-out", "b", new OrderCreated("o-b", 2))));

        Message<byte[]> first = outputDestination.receive(5000, "batch-out");
        Message<byte[]> second = outputDestination.receive(5000, "batch-out");

        assertThat(first).isNotNull();
        assertThat(second).isNotNull();
        assertThat(
                        List.of(
                                new String(first.getPayload(), UTF_8),
                                new String(second.getPayload(), UTF_8)))
                .anySatisfy(payload -> assertThat(payload).contains("\"orderId\":\"o-a\""))
                .anySatisfy(payload -> assertThat(payload).contains("\"orderId\":\"o-b\""));
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration(
            exclude = {
                DataSourceAutoConfiguration.class,
                DataSourceTransactionManagerAutoConfiguration.class,
                JdbcTemplateAutoConfiguration.class
            })
    static class TestApp {}
}
