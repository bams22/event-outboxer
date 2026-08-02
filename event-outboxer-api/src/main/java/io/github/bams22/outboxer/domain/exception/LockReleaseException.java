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

import org.jspecify.annotations.Nullable;

/**
 * The locker could not release a previously acquired lock. The library swallows this exception
 * inside {@code LockHandle.close()} (ADR Q5: {@code close()} is declared without {@code throws})
 * and instead logs a warning and emits {@code OutboxListener.onLockReleaseFailed(...)}. Users
 * rarely see this type directly; it exists for adapter implementations to throw internally.
 */
public final class LockReleaseException extends LockException {

  private static final long serialVersionUID = 1L;

  /** Message code used as a prefix in error text: {@value}. */
  public static final String CODE = "OUTBOX-402";

  public LockReleaseException(String message, @Nullable Throwable cause) {
    super(CODE + ": " + message, cause);
  }
}
