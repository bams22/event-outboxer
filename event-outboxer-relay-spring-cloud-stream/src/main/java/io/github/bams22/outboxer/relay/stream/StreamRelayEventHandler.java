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

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.domain.EventType;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import lombok.Builder;
import org.jspecify.annotations.Nullable;
import org.springframework.cloud.stream.function.StreamOperations;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;

/**
 * The built-in relay handler (ADR-0032): delivers stored {@link StreamEnvelope} events to their
 * Spring Cloud Stream binding through {@code StreamBridge}. Registered automatically by the
 * module's auto-configuration; the engine picks it up like any other {@code EventHandler}, so
 * retries, per-type tuning ({@code event-outboxer.event-types.overrides.outboxer-stream-relay}),
 * tracing and metrics all apply.
 *
 * <p>Header assembly: envelope headers are copied first, then the {@code contentType} header and
 * the configured message-key header are set on top — the explicit envelope fields win on collision.
 * The key is written as UTF-8 bytes, matching the Kafka binder's default {@code
 * ByteArraySerializer} key serializer.
 *
 * <p>Delivery is idempotent only in the at-least-once sense (ADR-0015): a crash between {@code
 * send} and finalize redelivers the message — consumers must deduplicate.
 *
 * <p>With {@code perKeyOrdering} enabled, {@link #extractLockKey} serializes handling of events
 * that share a (binding, key) pair through the configured {@code EntityLocker} (ADR-0012) —
 * relay-level ordering, which requires {@code event-outboxer.lock.type} to be set to a real locker.
 *
 * <p><b>Construction.</b> {@code StreamRelayEventHandler.builder()} — see the constructor for
 * required collaborators and defaults.
 */
public final class StreamRelayEventHandler implements EventHandler<StreamEnvelope> {

    /**
     * Default message-key header name: {@value} — the Kafka binder's record-key header. Other
     * binders either ignore it or need {@code messageKeyHeader} reconfigured.
     */
    public static final String DEFAULT_MESSAGE_KEY_HEADER = "kafka_messageKey";

    private static final String LOCK_KEY_PREFIX = "outboxer-stream-relay:";

    private final StreamOperations streamOperations;
    private final @Nullable String messageKeyHeader;
    private final boolean perKeyOrdering;

    /**
     * Builder-backed constructor; parameter names are the builder's method names. Required: {@code
     * streamOperations} (the {@code StreamBridge}). Defaults: {@code messageKeyHeader} — {@link
     * #DEFAULT_MESSAGE_KEY_HEADER}, blank disables the key header entirely (rely on the binding's
     * {@code partitionKeyExpression} instead); {@code perKeyOrdering} — {@code false}.
     */
    @Builder
    private StreamRelayEventHandler(
            StreamOperations streamOperations,
            @Nullable String messageKeyHeader,
            @Nullable Boolean perKeyOrdering) {
        this.streamOperations =
                Objects.requireNonNull(streamOperations, "streamOperations must not be null");
        this.messageKeyHeader =
                messageKeyHeader == null
                        ? DEFAULT_MESSAGE_KEY_HEADER
                        : messageKeyHeader.isBlank() ? null : messageKeyHeader;
        this.perKeyOrdering = perKeyOrdering != null && perKeyOrdering;
    }

    @Override
    public EventType<StreamEnvelope> type() {
        return StreamEnvelope.EVENT_TYPE;
    }

    @Override
    public EventOutcome handle(EventContext ctx, StreamEnvelope envelope) {
        Object wirePayload =
                envelope.textPayload() != null ? envelope.textPayload() : envelope.binaryPayload();
        MessageBuilder<Object> messageBuilder =
                MessageBuilder.withPayload(Objects.requireNonNull(wirePayload))
                        .copyHeaders(envelope.headers())
                        .setHeader(MessageHeaders.CONTENT_TYPE, envelope.contentType());
        if (messageKeyHeader != null && envelope.key() != null) {
            messageBuilder.setHeader(
                    messageKeyHeader, envelope.key().getBytes(StandardCharsets.UTF_8));
        }
        Message<Object> message = messageBuilder.build();
        boolean sent = streamOperations.send(envelope.binding(), message);
        return sent
                ? EventOutcome.success()
                : EventOutcome.retry(
                        "StreamBridge.send returned false for binding '"
                                + envelope.binding()
                                + "'");
    }

    @Override
    public @Nullable String extractLockKey(StreamEnvelope envelope) {
        return perKeyOrdering && envelope.key() != null
                ? LOCK_KEY_PREFIX + envelope.binding() + ":" + envelope.key()
                : null;
    }
}
