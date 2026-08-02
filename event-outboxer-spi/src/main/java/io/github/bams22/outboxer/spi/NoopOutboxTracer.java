/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spi;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * No-op {@link OutboxTracer} implementation — exposed as the {@link OutboxTracer#NOOP} singleton.
 *
 * <p>Publish handles carry an empty context map, process handles activate nothing; every method is
 * a zero-cost no-op so the engine never needs a null check on the tracing path.
 */
final class NoopOutboxTracer implements OutboxTracer {

    private static final PublishSpan PUBLISH_SPAN =
            new PublishSpan() {
                @Override
                public Map<String, String> contextToStore() {
                    return Map.of();
                }

                @Override
                public void coalesced(UUID existingEventId) {}

                @Override
                public void error(Throwable error) {}

                @Override
                public void close() {}
            };

    private static final ProcessSpan PROCESS_SPAN =
            new ProcessSpan() {
                @Override
                public void error(Throwable error) {}

                @Override
                public void close() {}
            };

    @Override
    public PublishSpan startPublishSpan(UUID eventId, String eventType) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        return PUBLISH_SPAN;
    }

    @Override
    public ProcessSpan startProcessSpan(ProcessSpanInfo info) {
        Objects.requireNonNull(info, "info must not be null");
        return PROCESS_SPAN;
    }
}
