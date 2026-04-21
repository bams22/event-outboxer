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
 * A claimed event could not be dispatched because no {@code EventHandler} is registered for its
 * {@code eventType}. Final action depends on the configured {@code UnknownHandlerPolicy}:
 * {@code SKIP} (leave as PENDING), {@code DISABLE} (default — mark as DISABLED), or {@code FAIL}
 * (log WARN and continue).
 */
public final class UnknownEventTypeException extends HandleException {

  private static final long serialVersionUID = 1L;

  /** Message code used as a prefix in error text: {@value}. */
  public static final String CODE = "OUTBOX-201";

  private final String eventType;

  public UnknownEventTypeException(String eventType) {
    super(CODE + ": no handler registered for event type '" + eventType + "'");
    this.eventType = eventType;
  }

  /** The event type that no handler is registered for. */
  public String eventType() {
    return eventType;
  }
}
