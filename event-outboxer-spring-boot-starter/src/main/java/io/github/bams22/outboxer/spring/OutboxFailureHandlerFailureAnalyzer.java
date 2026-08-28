/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring;

import org.jspecify.annotations.Nullable;
import org.springframework.boot.diagnostics.FailureAnalysis;
import org.springframework.boot.diagnostics.FailureAnalyzer;
import org.springframework.core.Ordered;

/**
 * Turns {@link AmbiguousOutboxFailureHandlerException} into a diagnosis naming the conflicting
 * beans and the one-bean-per-slot rule (ADR-0030).
 */
public class OutboxFailureHandlerFailureAnalyzer implements FailureAnalyzer, Ordered {

    @Override
    public int getOrder() {
        // Ahead of Boot's generic BeanCreationException analyzers.
        return Ordered.HIGHEST_PRECEDENCE + 100;
    }

    @Override
    public @Nullable FailureAnalysis analyze(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof AmbiguousOutboxFailureHandlerException ambiguous) {
                return new FailureAnalysis(ambiguous.getMessage(), action(), failure);
            }
        }
        return null;
    }

    private static String action() {
        return "Keep exactly one @OutboxFailureHandler bean without a value (the global chain) and"
                + " at most one bean per event type (@OutboxFailureHandler(\"TYPE\")). When you"
                + " use the annotation, drop the legacy outboxDefaultFailureHandler /"
                + " outboxPerTypeFailureHandlers beans — or configure the policy in YAML under"
                + " event-outboxer.event-types.defaults.failure.* and"
                + " event-outboxer.event-types.overrides.TYPE.failure.* instead.";
    }
}
