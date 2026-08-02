/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.publish;

/**
 * Detects whether the current thread is executing inside a transaction. The publisher consults this
 * port before every write so that the configured {@link NoTransactionPolicy} can fire on
 * violations.
 *
 * <p>The default plain-Java implementation (see {@link #alwaysActive()}) assumes every caller is
 * already in a transaction — suitable for tests and scripts. The Spring Boot starter substitutes an
 * implementation backed by {@code TransactionSynchronizationManager.isActualTransactionActive()}.
 */
@FunctionalInterface
public interface TransactionContext {

    /** {@code true} when a transaction is active on the calling thread. */
    boolean isActive();

    /**
     * Run {@code action} after the surrounding transaction commits. Used by the publisher to wake
     * the local poller of a just-published event type only once the inserted row is actually
     * visible to a claim query.
     *
     * <p>Contract for implementations that are aware of a real transaction manager: defer the
     * action until the transaction commits, and do NOT run it when the transaction rolls back.
     *
     * <p>The default runs the action immediately — best effort for plain-Java setups where no
     * transaction manager is observable. If the caller is inside an externally managed transaction,
     * an immediate wake merely triggers one empty poll (the row is not yet visible); that is a
     * wasted query, never a correctness problem.
     */
    default void afterCommit(Runnable action) {
        action.run();
    }

    /** No-op implementation that always reports "active". Used in plain-Java setups and tests. */
    static TransactionContext alwaysActive() {
        return () -> true;
    }

    /** Implementation that always reports "no active transaction" — useful for negative tests. */
    static TransactionContext neverActive() {
        return () -> false;
    }
}
