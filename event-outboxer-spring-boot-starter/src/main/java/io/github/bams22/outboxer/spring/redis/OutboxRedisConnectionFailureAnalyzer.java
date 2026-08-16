/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.redis;

import org.jspecify.annotations.Nullable;
import org.springframework.boot.diagnostics.FailureAnalysis;
import org.springframework.boot.diagnostics.FailureAnalyzer;
import org.springframework.core.Ordered;

/**
 * Renders {@link AmbiguousOutboxRedisConnectionException} — a Redis-backed outbox feature with no
 * resolvable {@code StatefulRedisConnection} (ADR-0027) — as an actionable startup diagnosis
 * instead of a stack-trace puzzle.
 */
public class OutboxRedisConnectionFailureAnalyzer implements FailureAnalyzer, Ordered {

    @Override
    public int getOrder() {
        // Ahead of Boot's generic NoUniqueBeanDefinitionException analyzer.
        return Ordered.HIGHEST_PRECEDENCE + 100;
    }

    @Override
    public @Nullable FailureAnalysis analyze(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof AmbiguousOutboxRedisConnectionException ambiguous) {
                return new FailureAnalysis(ambiguous.getMessage(), action(), failure);
            }
        }
        return null;
    }

    private static String action() {
        return "Set event-outboxer.redis.uri (or .host) so the starter creates the Lettuce"
                + " connection itself, or define exactly one StatefulRedisConnection<String,"
                + " String> bean the outbox should use — marking it with"
                + " @io.github.bams22.outboxer.spring.OutboxRedisConnection (or @Primary) when"
                + " several connections exist. Defining your own EntityLocker /"
                + " MetricsSnapshotCache beans also overrides the outbox Redis wiring entirely.";
    }
}
