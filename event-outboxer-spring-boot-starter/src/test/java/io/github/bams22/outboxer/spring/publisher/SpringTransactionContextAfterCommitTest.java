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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionSynchronizationUtils;

/**
 * The after-commit contract of {@link SpringTransactionContext}: inside a synchronized
 * transaction the action is deferred to the commit callback and dropped on rollback; without a
 * transaction it runs immediately.
 */
class SpringTransactionContextAfterCommitTest {

  private final SpringTransactionContext ctx = new SpringTransactionContext();

  @AfterEach
  void cleanupSynchronization() {
    if (TransactionSynchronizationManager.isSynchronizationActive()) {
      TransactionSynchronizationManager.clearSynchronization();
    }
  }

  @Test
  @DisplayName("no active synchronization → action runs immediately")
  void runsImmediatelyWithoutTransaction() {
    AtomicInteger runs = new AtomicInteger();

    ctx.afterCommit(runs::incrementAndGet);

    assertThat(runs).hasValue(1);
  }

  @Test
  @DisplayName("active synchronization → action deferred until afterCommit fires")
  void deferredUntilCommit() {
    TransactionSynchronizationManager.initSynchronization();
    AtomicInteger runs = new AtomicInteger();

    ctx.afterCommit(runs::incrementAndGet);
    assertThat(runs).hasValue(0);

    for (TransactionSynchronization sync :
        TransactionSynchronizationManager.getSynchronizations()) {
      sync.afterCommit();
    }
    assertThat(runs).hasValue(1);
  }

  @Test
  @DisplayName("rollback → action never runs")
  void droppedOnRollback() {
    TransactionSynchronizationManager.initSynchronization();
    AtomicInteger runs = new AtomicInteger();

    ctx.afterCommit(runs::incrementAndGet);

    // Simulate a rollback completion: afterCompletion(STATUS_ROLLED_BACK), no afterCommit.
    TransactionSynchronizationUtils.invokeAfterCompletion(
        TransactionSynchronizationManager.getSynchronizations(),
        TransactionSynchronization.STATUS_ROLLED_BACK);

    assertThat(runs).hasValue(0);
  }
}
