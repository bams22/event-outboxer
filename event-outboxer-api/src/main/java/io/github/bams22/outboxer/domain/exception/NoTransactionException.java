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
 * {@code publish(...)} was called outside an active transaction and the configuration requires
 * one ({@code outbox.publisher.no-transaction-policy=FAIL}, the default).
 *
 * <p>This exception guards the core outbox guarantee: a published event must commit or roll back
 * atomically with the caller's business data. Publishing outside a transaction would break that
 * invariant silently, so the default policy refuses to do it.
 */
public final class NoTransactionException extends PublishException {

  private static final long serialVersionUID = 1L;

  /** Message code used as a prefix in error text: {@value}. */
  public static final String CODE = "OUTBOX-103";

  public NoTransactionException(String message) {
    super(CODE + ": " + message);
  }
}
