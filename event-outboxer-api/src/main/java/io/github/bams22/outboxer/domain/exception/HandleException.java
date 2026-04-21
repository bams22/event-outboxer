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
 * Base class for engine-side failures while turning a claimed event into a handler invocation:
 * unknown event type, payload deserialization errors, and similar dispatch-time problems. Not
 * thrown back to the application — the engine catches it, routes the event through its failure
 * handler chain (disable or retry), and continues.
 */
public abstract class HandleException extends OutboxException {

  private static final long serialVersionUID = 1L;

  protected HandleException(String message) {
    super(message);
  }

  protected HandleException(String message, Throwable cause) {
    super(message, cause);
  }
}
