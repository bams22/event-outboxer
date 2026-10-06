/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.domain.exception;

/**
 * {@code OutboxEventTracker.await(...)} was called while a transaction is active on the calling
 * thread. The awaited row becomes visible to the engine only after the publishing transaction
 * commits (ADR-0002), so such a wait could only run into its timeout; the tracker fails fast
 * instead (ADR-0038).
 *
 * <p>Transaction synchronisation callbacks ({@code afterCommit}, {@code afterCompletion}) are also
 * reported as inside the transaction: Spring clears the transaction state only after they run, and
 * waiting there would hold the transaction's connection for the whole wait. Call {@code await}
 * once the transactional method has returned.
 */
public final class AwaitInTransactionException extends TrackingException {

    private static final long serialVersionUID = 1L;

    /**
     * Message code used as a prefix in error text: {@value}.
     */
    public static final String CODE = "OUTBOX-501";

    public AwaitInTransactionException(String message) {
        super(CODE + ": " + message);
    }
}
