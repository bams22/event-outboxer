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
 * Two or more {@code EventHandler} beans claim the same {@code eventType} at startup. The engine
 * refuses to start because the ambiguity cannot be resolved safely — an event of that type might
 * be dispatched to either handler.
 */
public final class DuplicateHandlerException extends ConfigurationException {

  private static final long serialVersionUID = 1L;

  private final String eventType;

  public DuplicateHandlerException(String eventType, String message) {
    super(message);
    this.eventType = eventType;
  }

  /** The event type that has multiple handlers registered for it. */
  public String eventType() {
    return eventType;
  }
}
