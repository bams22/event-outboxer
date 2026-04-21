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
 * Startup-time failure: a per-event-type configuration block is invalid. Examples: negative
 * pool sizes, {@code multiplier <= 1.0} with {@code strategy=EXPONENTIAL}, jitter outside
 * {@code [0, 1]}, or a {@code batch-size} larger than {@code max-pool-size + queue-capacity}.
 */
public final class InvalidEventTypeConfigException extends ConfigurationException {

  private static final long serialVersionUID = 1L;

  public InvalidEventTypeConfigException(String message) {
    super(message);
  }

  public InvalidEventTypeConfigException(String message, Throwable cause) {
    super(message, cause);
  }
}
