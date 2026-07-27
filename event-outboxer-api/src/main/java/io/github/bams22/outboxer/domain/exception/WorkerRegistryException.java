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
 * A {@code WorkerRegistry} operation (register, heartbeat, findDead, deregister, ...) failed for a
 * technical reason. Wraps the adapter-specific cause.
 */
public final class WorkerRegistryException extends StorageException {

  private static final long serialVersionUID = 1L;

  /** Message code used as a prefix in error text: {@value}. */
  public static final String CODE = "OUTBOX-303";

  public WorkerRegistryException(String message, Throwable cause) {
    super(CODE + ": " + message, cause);
  }
}
