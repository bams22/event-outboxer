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
 * Publish call was rejected because of invalid input — for example a blank {@code eventType}, a
 * {@code null} payload, or a {@code runAt} in the past when the configuration forbids it.
 */
public final class PublishValidationException extends PublishException {

  private static final long serialVersionUID = 1L;

  /** Message code used as a prefix in error text: {@value}. */
  public static final String CODE = "OUTBOX-101";

  public PublishValidationException(String message) {
    super(CODE + ": " + message);
  }

  public PublishValidationException(String message, @Nullable Throwable cause) {
    super(CODE + ": " + message, cause);
  }
}
