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
 * Detects whether the current thread is executing inside a transaction. The publisher consults
 * this port before every write so that the configured
 * {@link NoTransactionPolicy} can fire on violations.
 *
 * <p>The default plain-Java implementation (see {@link #alwaysActive()}) assumes every caller is
 * already in a transaction — suitable for tests and scripts. The Spring Boot starter substitutes
 * an implementation backed by {@code TransactionSynchronizationManager.isActualTransactionActive()}.
 */
@FunctionalInterface
public interface TransactionContext {

  /** {@code true} when a transaction is active on the calling thread. */
  boolean isActive();

  /** No-op implementation that always reports "active". Used in plain-Java setups and tests. */
  static TransactionContext alwaysActive() {
    return () -> true;
  }

  /** Implementation that always reports "no active transaction" — useful for negative tests. */
  static TransactionContext neverActive() {
    return () -> false;
  }
}
