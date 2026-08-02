/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.publisher;

import io.github.bams22.outboxer.core.publish.TransactionContext;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * {@link TransactionContext} backed by Spring's {@link
 * TransactionSynchronizationManager#isActualTransactionActive()}. Returns {@code true} when the
 * caller is inside a physical transaction (typically under
 * {@code @Transactional(PROPAGATION_REQUIRED)}), {@code false} otherwise.
 *
 * <p>{@link #afterCommit(Runnable)} defers the action to a Spring after-commit synchronization, so
 * the publisher's poller wake-up fires only once the inserted event row is actually visible — and
 * never fires at all when the transaction rolls back.
 */
public final class SpringTransactionContext implements TransactionContext {

    @Override
    public boolean isActive() {
        return TransactionSynchronizationManager.isActualTransactionActive();
    }

    @Override
    public void afterCommit(Runnable action) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(
                    new TransactionSynchronization() {
                        @Override
                        public void afterCommit() {
                            action.run();
                        }
                    });
        } else {
            // No surrounding transaction (noTransactionPolicy=IGNORE): the insert already
            // auto-committed, an immediate wake is correct.
            action.run();
        }
    }
}
