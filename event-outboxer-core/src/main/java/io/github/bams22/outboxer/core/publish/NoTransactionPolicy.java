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
 * What the publisher does when {@link TransactionContext#isActive()} reports no active
 * transaction. See {@code OutboxEventPublisher}'s Javadoc for the full contract.
 */
public enum NoTransactionPolicy {

  /** Throw {@code NoTransactionException}. Default — matches the transactional-outbox pattern. */
  FAIL,

  /**
   * Proceed without a surrounding transaction. Loses atomicity with the caller's state. Useful
   * for maintenance scripts.
   */
  IGNORE
}
