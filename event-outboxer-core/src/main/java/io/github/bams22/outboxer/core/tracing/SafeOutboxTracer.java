/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.tracing;

import io.github.bams22.outboxer.spi.OutboxTracer;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defensive {@link OutboxTracer} decorator (ADR-0023). Catches every {@link RuntimeException} an
 * adapter may throw — from span start, context capture, error recording, or close — logs it at
 * debug and degrades to a no-op, so a broken tracing backend can never fail a publish or leave an
 * event stuck in PROCESSING.
 *
 * <p>The engine wraps the configured tracer exactly once via {@link #wrap(OutboxTracer)}; the
 * publisher and dispatcher then call the port without any tracing-specific try/catch of their own.
 */
public final class SafeOutboxTracer implements OutboxTracer {

    private static final Logger log = LoggerFactory.getLogger(SafeOutboxTracer.class);

    private final OutboxTracer delegate;

    private SafeOutboxTracer(OutboxTracer delegate) {
        this.delegate = delegate;
    }

    /**
     * Wraps {@code tracer} in a defensive decorator. Identity for {@link OutboxTracer#NOOP}
     * (nothing to shield) and for tracers that are already wrapped.
     */
    public static OutboxTracer wrap(OutboxTracer tracer) {
        Objects.requireNonNull(tracer, "tracer must not be null");
        if (tracer == OutboxTracer.NOOP || tracer instanceof SafeOutboxTracer) {
            return tracer;
        }
        return new SafeOutboxTracer(tracer);
    }

    @Override
    public PublishSpan startPublishSpan(UUID eventId, String eventType) {
        try {
            return new SafePublishSpan(delegate.startPublishSpan(eventId, eventType));
        } catch (RuntimeException ex) {
            log.debug("OutboxTracer.startPublishSpan failed: {}", ex.toString());
            return NOOP.startPublishSpan(eventId, eventType);
        }
    }

    @Override
    public ProcessSpan startProcessSpan(ProcessSpanInfo info) {
        try {
            return new SafeProcessSpan(delegate.startProcessSpan(info));
        } catch (RuntimeException ex) {
            log.debug("OutboxTracer.startProcessSpan failed: {}", ex.toString());
            return NOOP.startProcessSpan(info);
        }
    }

    private static final class SafePublishSpan implements PublishSpan {

        private final PublishSpan delegate;

        private SafePublishSpan(PublishSpan delegate) {
            this.delegate = delegate;
        }

        @Override
        public Map<String, String> contextToStore() {
            try {
                // contextToStore() is non-null by contract, but this wrapper's whole job is
                // shielding
                // the engine from misbehaving third-party tracer adapters — including a null
                // return.
                Map<String, String> context = delegate.contextToStore();
                return context != null ? context : Map.of();
            } catch (RuntimeException ex) {
                log.debug("PublishSpan.contextToStore failed: {}", ex.toString());
                return Map.of();
            }
        }

        @Override
        public void coalesced(UUID existingEventId) {
            try {
                delegate.coalesced(existingEventId);
            } catch (RuntimeException ex) {
                log.debug("PublishSpan.coalesced failed: {}", ex.toString());
            }
        }

        @Override
        public void linked() {
            try {
                delegate.linked();
            } catch (RuntimeException ex) {
                log.debug("PublishSpan.linked failed: {}", ex.toString());
            }
        }

        @Override
        public void error(Throwable error) {
            try {
                delegate.error(error);
            } catch (RuntimeException ex) {
                log.debug("PublishSpan.error failed: {}", ex.toString());
            }
        }

        @Override
        public void close() {
            try {
                delegate.close();
            } catch (RuntimeException ex) {
                log.debug("PublishSpan.close failed: {}", ex.toString());
            }
        }
    }

    private static final class SafeProcessSpan implements ProcessSpan {

        private final ProcessSpan delegate;

        private SafeProcessSpan(ProcessSpan delegate) {
            this.delegate = delegate;
        }

        @Override
        public void error(Throwable error) {
            try {
                delegate.error(error);
            } catch (RuntimeException ex) {
                log.debug("ProcessSpan.error failed: {}", ex.toString());
            }
        }

        @Override
        public void close() {
            try {
                delegate.close();
            } catch (RuntimeException ex) {
                log.debug("ProcessSpan.close failed: {}", ex.toString());
            }
        }
    }
}
