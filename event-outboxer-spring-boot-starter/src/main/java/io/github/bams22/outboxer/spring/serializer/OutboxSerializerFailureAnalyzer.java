/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.serializer;

import io.github.bams22.outboxer.domain.exception.NoEventSerializersException;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.diagnostics.FailureAnalysis;
import org.springframework.boot.diagnostics.FailureAnalyzer;
import org.springframework.core.Ordered;

/**
 * Turns the "no EventSerializer bean" startup failure into a diagnosis. JSON via Jackson ships with
 * the starter (ADR-0016, amendment 2026-08-29), so reaching this failure means the module was
 * excluded — or its auto-configuration disabled — without another serializer taking its place.
 */
public class OutboxSerializerFailureAnalyzer implements FailureAnalyzer, Ordered {

    @Override
    public int getOrder() {
        // Ahead of Boot's generic BeanCreationException analyzers.
        return Ordered.HIGHEST_PRECEDENCE + 100;
    }

    @Override
    public @Nullable FailureAnalysis analyze(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof NoEventSerializersException) {
                return new FailureAnalysis(description(), action(), failure);
            }
        }
        return null;
    }

    private static String description() {
        return "The outbox has no EventSerializer bean, so nothing can serialize event payloads."
                + " JSON via Jackson (event-outboxer-serializer-jackson) comes with"
                + " event-outboxer-spring-boot-starter by default; this failure means it was"
                + " excluded from the starter, or its auto-configuration was disabled, without"
                + " another serializer taking its place.";
    }

    private static String action() {
        return "Remove the exclusion of event-outboxer-serializer-jackson from"
                + " event-outboxer-spring-boot-starter, add event-outboxer-serializer-protobuf"
                + " (with protobuf-java on the classpath) for a protobuf-only setup, or declare"
                + " your own EventSerializer bean. To write another format while keeping Jackson"
                + " on the classpath for reads, do not exclude the module — set"
                + " event-outboxer.serializer.write-format instead.";
    }
}
