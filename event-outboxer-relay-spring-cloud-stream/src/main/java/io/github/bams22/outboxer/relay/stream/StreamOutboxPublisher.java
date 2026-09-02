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

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Transactional publisher of broker messages through the outbox (ADR-0032): stores a {@link
 * StreamEnvelope} event inside the caller's transaction; the built-in relay handler delivers it to
 * the Spring Cloud Stream binding after commit, with the engine's full at-least-once machinery
 * (retries, per-type tuning, tracing, metrics) behind it.
 *
 * <p>Delegates to {@code OutboxEventPublisher}, so all its transaction-participation semantics
 * apply verbatim (ADR-0002, {@code event-outboxer.publisher.no-transaction-policy}), and failures
 * throw the same {@code PublishException} subclasses plus {@link StreamEncodingException} for
 * payload-encoding errors.
 *
 * <h2>Payload encoding</h2>
 *
 * <p>The payload is encoded to its wire form at publish time and stored; delivery ships the stored
 * form verbatim. An explicit DTO (ADR-0003) goes through the configured {@link
 * StreamPayloadEncoder} (JSON by default). Pre-encoded payloads pass through untouched: a {@code
 * String}, {@code byte[]} or {@code SerializedPayload} payload is treated as the wire form itself —
 * note a {@code String} is NOT JSON-quoted, and pre-encoded payloads should carry an explicit
 * {@code contentType}.
 *
 * <p>Duplicate deliveries are possible (at-least-once, ADR-0015) — consumers must deduplicate,
 * exactly as with any outbox relay.
 */
public interface StreamOutboxPublisher {

    /**
     * Publish a payload to a binding with a message key and defaults for everything else.
     * Equivalent to {@code publish(StreamOutboxMessage.of(binding, key, payload))}.
     *
     * @return id of the stored outbox event
     */
    UUID publish(String binding, @Nullable String key, Object payload);

    /**
     * Publish the full form — headers, content-type override, per-call {@code PublishOptions}.
     *
     * @return id of the stored outbox event
     */
    UUID publish(StreamOutboxMessage message);

    /**
     * Publish a batch in one storage round-trip. Fail-fast and all-or-nothing with the same
     * semantics as {@code OutboxEventPublisher.publishAll(...)}.
     *
     * @return ids of the stored events, in iteration order of {@code messages}
     */
    List<UUID> publishAll(Collection<StreamOutboxMessage> messages);
}
