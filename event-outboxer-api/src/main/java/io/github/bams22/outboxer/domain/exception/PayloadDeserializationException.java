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
 * The engine could not reconstruct the payload of a claimed event — typically the stored shape is
 * incompatible with the current {@code EventHandler.payloadType()}. This surfaces most often during
 * a rolling deploy that changes a DTO without a compatible evolution (for example, removing a field
 * without a default).
 *
 * <p>Events that fail deserialization are routed through the failure handler chain and normally end
 * up in {@code DISABLED} for manual investigation.
 */
public final class PayloadDeserializationException extends HandleException {

  private static final long serialVersionUID = 1L;

  /** Message code used as a prefix in error text: {@value}. */
  public static final String CODE = "OUTBOX-202";

  public PayloadDeserializationException(String message, @Nullable Throwable cause) {
    super(CODE + ": " + message, cause);
  }
}
