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
 * The publisher could not serialize the payload to its persisted form. Typically wraps a Jackson
 * {@code JsonProcessingException} from the configured {@code EventSerializer}.
 */
public final class PublishSerializationException extends PublishException {

  private static final long serialVersionUID = 1L;

  /** Message code used as a prefix in error text: {@value}. */
  public static final String CODE = "OUTBOX-102";

  public PublishSerializationException(String message, @Nullable Throwable cause) {
    super(CODE + ": " + message, cause);
  }
}
