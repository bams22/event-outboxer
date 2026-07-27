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
 * Base class for startup-time configuration errors. Raised by the Spring Boot starter's property
 * validator and by core engine initialization when invariants (for example {@code deadThreshold >=
 * 3 * heartbeatInterval}) are violated or when two {@code EventHandler} beans claim the same {@code
 * eventType}.
 *
 * <p>All subclasses are fail-fast: they are thrown during application startup and should prevent
 * the process from becoming "ready" for traffic.
 */
public abstract class ConfigurationException extends OutboxException {

  private static final long serialVersionUID = 1L;

  protected ConfigurationException(String message) {
    super(message);
  }

  protected ConfigurationException(String message, Throwable cause) {
    super(message, cause);
  }
}
