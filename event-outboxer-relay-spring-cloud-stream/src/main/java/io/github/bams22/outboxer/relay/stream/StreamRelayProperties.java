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

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for the Spring Cloud Stream relay (ADR-0032), bound from {@code
 * event-outboxer.relay.stream.*}.
 */
@Getter
@Setter
@ConfigurationProperties("event-outboxer.relay.stream")
public class StreamRelayProperties {

    /**
     * Kill switch. The relay activates automatically when the module and {@code StreamBridge} are
     * on the classpath; set to {@code false} to keep the jar without the relay beans.
     */
    private boolean enabled = true;

    /**
     * Name of the outgoing message header carrying the message key, written as UTF-8 bytes. The
     * default targets the Kafka binder's record-key header; set to an empty string to disable the
     * key header entirely and rely on the binding's {@code partitionKeyExpression} instead.
     */
    private String messageKeyHeader = StreamRelayEventHandler.DEFAULT_MESSAGE_KEY_HEADER;

    /**
     * Serialize relay deliveries that share a (binding, key) pair through the configured entity
     * locker (ADR-0012). Requires {@code event-outboxer.lock.type} to be a real locker; costs
     * throughput.
     */
    private boolean perKeyOrdering = false;

    /**
     * Content type stamped on messages whose payload was passed pre-encoded ({@code String}, {@code
     * byte[]}, {@code SerializedPayload}) without an explicit per-message content type. Encoded
     * payloads use the encoder's own content type instead.
     */
    private String defaultContentType = StreamPayloadEncoder.DEFAULT_CONTENT_TYPE;
}
