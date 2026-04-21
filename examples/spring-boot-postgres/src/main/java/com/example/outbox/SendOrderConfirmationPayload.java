package com.example.outbox;

import java.util.UUID;

/** Event payload — serialized as JSON and stored in {@code outbox.events.payload}. */
public record SendOrderConfirmationPayload(UUID orderId, String email, long totalCents) {

  /** Event type string — must match the handler's {@code eventType()}. */
  public static final String EVENT_TYPE = "SEND_ORDER_CONFIRMATION";
}
