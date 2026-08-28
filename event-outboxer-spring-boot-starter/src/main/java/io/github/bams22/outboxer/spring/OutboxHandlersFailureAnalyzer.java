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

import io.github.bams22.outboxer.domain.exception.NoEventHandlersException;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.diagnostics.FailureAnalysis;
import org.springframework.boot.diagnostics.FailureAnalyzer;
import org.springframework.core.Ordered;

/**
 * Turns the engine's refusal to start without any {@code EventHandler} bean into a diagnosis naming
 * the two ways out: declare a handler, or opt into publish-only mode with {@code
 * event-outboxer.publish-only=true} (ADR-0029).
 */
public class OutboxHandlersFailureAnalyzer implements FailureAnalyzer, Ordered {

    @Override
    public int getOrder() {
        // Ahead of Boot's generic BeanCreationException analyzers.
        return Ordered.HIGHEST_PRECEDENCE + 100;
    }

    @Override
    public @Nullable FailureAnalysis analyze(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof NoEventHandlersException) {
                return new FailureAnalysis(description(), action(), failure);
            }
        }
        return null;
    }

    private static String description() {
        return "The outbox engine found no EventHandler beans, so every event this application"
                + " publishes would stay PENDING forever — almost always a wiring mistake (handlers"
                + " outside component scan, wrong profile, missing @Component).";
    }

    private static String action() {
        return "Declare at least one EventHandler bean (a @Component implementing"
                + " EventHandler<T>). If this application only publishes events and the handlers"
                + " run in another deployment, set event-outboxer.publish-only=true: the engine"
                + " then registers its worker and runs maintenance but starts no pollers.";
    }
}
