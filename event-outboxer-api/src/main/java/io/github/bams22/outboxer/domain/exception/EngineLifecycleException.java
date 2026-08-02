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
 * Base class for engine lifecycle misuse — methods called before {@code start()}, after {@code
 * stop()}, or in other states that do not support them. Typically indicates a programming error in
 * the integrating code.
 */
public abstract class EngineLifecycleException extends OutboxException {

  private static final long serialVersionUID = 1L;

  protected EngineLifecycleException(String message) {
    super(message);
  }

  protected EngineLifecycleException(String message, @Nullable Throwable cause) {
    super(message, cause);
  }
}
