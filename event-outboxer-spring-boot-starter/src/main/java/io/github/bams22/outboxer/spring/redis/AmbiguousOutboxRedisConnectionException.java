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

import io.lettuce.core.api.StatefulRedisConnection;
import java.util.Collection;
import java.util.List;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;

/**
 * Thrown when the outbox cannot pick a {@link StatefulRedisConnection} (ADR-0027): none exists and
 * no {@code event-outboxer.redis.*} properties are set, several beans carry
 * {@code @OutboxRedisConnection}, or several connections exist and none is qualified or
 * {@code @Primary}.
 *
 * <p>Extends {@link NoUniqueBeanDefinitionException} so Spring Boot's stock failure analyzer still
 * applies as a fallback; {@link OutboxRedisConnectionFailureAnalyzer} renders the dedicated
 * diagnosis.
 */
public class AmbiguousOutboxRedisConnectionException extends NoUniqueBeanDefinitionException {

    private final List<String> candidates;

    private AmbiguousOutboxRedisConnectionException(String message, Collection<String> candidates) {
        super(StatefulRedisConnection.class, candidates.size(), message);
        this.candidates = List.copyOf(candidates);
    }

    /** Two or more beans carry {@code @OutboxRedisConnection} — the qualifier must be unique. */
    static AmbiguousOutboxRedisConnectionException multipleQualified(
            Collection<String> candidates) {
        return new AmbiguousOutboxRedisConnectionException(
                "Found "
                        + candidates.size()
                        + " StatefulRedisConnection beans marked with @OutboxRedisConnection"
                        + (candidates.isEmpty() ? "" : ": " + candidates)
                        + " — exactly one bean may carry the qualifier, the outbox cannot choose"
                        + " between them.",
                candidates);
    }

    /**
     * Several {@code StatefulRedisConnection} beans exist and neither
     * {@code @OutboxRedisConnection} nor {@code @Primary} singles one out.
     */
    static AmbiguousOutboxRedisConnectionException noneQualified(Collection<String> candidates) {
        return new AmbiguousOutboxRedisConnectionException(
                "Found "
                        + candidates.size()
                        + " StatefulRedisConnection beans and none is @Primary or marked with"
                        + " @OutboxRedisConnection: "
                        + candidates
                        + ". The outbox cannot choose which Redis to use — mark exactly one bean"
                        + " with io.github.bams22.outboxer.spring.@OutboxRedisConnection (or"
                        + " declare one @Primary).",
                candidates);
    }

    /**
     * A Redis-backed feature is enabled ({@code lock.type=redis} or {@code cache.type=redis}) but
     * no connection exists at all and no {@code event-outboxer.redis.*} properties are set.
     */
    static AmbiguousOutboxRedisConnectionException noneAvailable() {
        return new AmbiguousOutboxRedisConnectionException(
                "A Redis-backed outbox feature is enabled (event-outboxer.lock.type=redis or"
                        + " event-outboxer.cache.type=redis) but no StatefulRedisConnection<String,"
                        + " String> is available. Set event-outboxer.redis.uri (or .host) so the"
                        + " starter creates the connection, or define a"
                        + " StatefulRedisConnection<String, String> bean — optionally marked with"
                        + " @OutboxRedisConnection.",
                List.of());
    }

    /** Bean names of the conflicting {@code StatefulRedisConnection} candidates. */
    public List<String> getCandidates() {
        return candidates;
    }
}
