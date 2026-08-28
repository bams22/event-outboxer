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

import io.github.bams22.outboxer.domain.exception.ConfigurationException;
import java.util.Collection;
import java.util.List;

/**
 * Thrown when several beans claim the same failure-chain slot (ADR-0030): two global chains, or two
 * chains for one event type — whether through {@link OutboxFailureHandler @OutboxFailureHandler} or
 * the legacy bean names. {@link OutboxFailureHandlerFailureAnalyzer} renders the diagnosis.
 */
public final class AmbiguousOutboxFailureHandlerException extends ConfigurationException {

    private static final long serialVersionUID = 1L;

    private final List<String> candidates;

    private AmbiguousOutboxFailureHandlerException(String message, Collection<String> candidates) {
        super(message);
        this.candidates = List.copyOf(candidates);
    }

    /** Two or more beans claim the global failure chain. */
    static AmbiguousOutboxFailureHandlerException multipleGlobal(Collection<String> candidates) {
        return new AmbiguousOutboxFailureHandlerException(
                "Found "
                        + candidates.size()
                        + " beans claiming the global outbox failure chain: "
                        + candidates
                        + " — exactly one may (a @OutboxFailureHandler bean without a value, or"
                        + " the legacy bean named outboxDefaultFailureHandler).",
                candidates);
    }

    /** Two or more beans claim the chain of one event type. */
    static AmbiguousOutboxFailureHandlerException multiplePerType(
            String eventType, Collection<String> candidates) {
        return new AmbiguousOutboxFailureHandlerException(
                "Found "
                        + candidates.size()
                        + " beans claiming the outbox failure chain for event type '"
                        + eventType
                        + "': "
                        + candidates
                        + " — exactly one may (@OutboxFailureHandler(\""
                        + eventType
                        + "\"), or one entry in the legacy outboxPerTypeFailureHandlers map).",
                candidates);
    }

    /**
     * Bean names (or {@code outboxPerTypeFailureHandlers[TYPE]} entries) of the conflicting claims.
     */
    public List<String> getCandidates() {
        return candidates;
    }
}
