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
 * A claimed event carries a {@code payloadFormat} for which no {@code EventSerializer} is
 * registered. This surfaces most often during a rolling deploy where an old replica claims an event
 * written by a newer replica in a format the old one does not know yet.
 *
 * <p>Like {@link PayloadDeserializationException}, the engine routes such events through the
 * failure handler chain (retry with backoff, eventually {@code DISABLED}) — a replica that knows
 * the format may pick the event up on a later attempt (ADR-0025).
 */
public final class UnknownPayloadFormatException extends HandleException {

  private static final long serialVersionUID = 1L;

  /** Message code used as a prefix in error text: {@value}. */
  public static final String CODE = "OUTBOX-203";

  public UnknownPayloadFormatException(String message) {
    super(CODE + ": " + message);
  }
}
