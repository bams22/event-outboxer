/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.example.outbox;

import io.github.bams22.outboxer.domain.EventType;
import java.util.UUID;

/** Event payload — serialized as JSON and stored in {@code event_outboxer.events.payload}. */
public record SendOrderConfirmationPayload(UUID orderId, String email, long totalCents) {

  /**
   * Typed event key shared by the handler ({@code type()}) and the publisher ({@code
   * publisher.publish(EVENT_TYPE, payload)}) — the name and the payload class are spelled once.
   */
  public static final EventType<SendOrderConfirmationPayload> EVENT_TYPE =
      EventType.of("SEND_ORDER_CONFIRMATION", SendOrderConfirmationPayload.class);
}
