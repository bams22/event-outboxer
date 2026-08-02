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
 * Base class for technical failures in the {@code EntityLocker} port. The normal "lock is currently
 * held by someone else" scenario is <strong>not</strong> an exception — it is represented by an
 * {@code Optional.empty()} result from {@code tryLock(...)} (see ADR-0030 summary in
 * decisions_exceptions.md).
 *
 * <p>{@code LockException} signals that the locker could not carry out the operation at all — for
 * example the Redis server is unreachable, the advisory-lock SQL failed, or the unlock script
 * encountered an unexpected error.
 */
public abstract class LockException extends OutboxException {

  private static final long serialVersionUID = 1L;

  protected LockException(String message) {
    super(message);
  }

  protected LockException(String message, @Nullable Throwable cause) {
    super(message, cause);
  }
}
