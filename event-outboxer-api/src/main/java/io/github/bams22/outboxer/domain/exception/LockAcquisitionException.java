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
 * The locker failed to acquire or even attempt a lock for a technical reason (Redis unreachable,
 * advisory-lock SQL error, connection problem). This is distinct from the ordinary "lock is busy"
 * outcome, which is represented by {@code Optional.empty()} from {@code tryLock}.
 */
public final class LockAcquisitionException extends LockException {

  private static final long serialVersionUID = 1L;

  /** Message code used as a prefix in error text: {@value}. */
  public static final String CODE = "OUTBOX-401";

  public LockAcquisitionException(String message, Throwable cause) {
    super(CODE + ": " + message, cause);
  }
}
