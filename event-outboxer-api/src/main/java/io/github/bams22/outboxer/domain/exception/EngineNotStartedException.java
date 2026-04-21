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
 * A method that requires the engine to be running was called before {@code OutboxEngine.start()}
 * (or after {@code stop()}). Almost always indicates a programming error in the integrating
 * code.
 */
public final class EngineNotStartedException extends EngineLifecycleException {

  private static final long serialVersionUID = 1L;

  public EngineNotStartedException(String message) {
    super(message);
  }
}
