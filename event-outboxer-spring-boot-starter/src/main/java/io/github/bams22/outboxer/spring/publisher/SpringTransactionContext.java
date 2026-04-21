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
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * {@link TransactionContext} backed by Spring's
 * {@link TransactionSynchronizationManager#isActualTransactionActive()}. Returns {@code true}
 * when the caller is inside a physical transaction (typically under
 * {@code @Transactional(PROPAGATION_REQUIRED)}), {@code false} otherwise.
 */
public final class SpringTransactionContext implements TransactionContext {

  @Override
  public boolean isActive() {
    return TransactionSynchronizationManager.isActualTransactionActive();
  }
}
