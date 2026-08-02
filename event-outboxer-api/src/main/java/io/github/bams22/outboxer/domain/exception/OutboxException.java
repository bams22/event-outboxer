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
 * Root of every exception thrown by the event-outboxer library. Always unchecked ({@link
 * RuntimeException}) — we avoid checked exceptions in public contracts because they tend to leak
 * into lambdas and records that make up much of the library's API surface.
 *
 * <p>Operational exceptions carry an {@code OUTBOX-XXX} message-code prefix to make log greps and
 * troubleshooting docs straightforward. Validation and configuration exceptions do not require a
 * code — their class name is specific enough.
 */
public abstract class OutboxException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  protected OutboxException(String message) {
    super(message);
  }

  protected OutboxException(String message, @Nullable Throwable cause) {
    super(message, cause);
  }
}
