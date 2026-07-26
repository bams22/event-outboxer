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

import java.util.UUID;

/** Event payload — serialized as JSON and stored in {@code outbox.events.payload}. */
public record SendOrderConfirmationPayload(UUID orderId, String email, long totalCents) {

  /** Event type string — must match the handler's {@code eventType()}. */
  public static final String EVENT_TYPE = "SEND_ORDER_CONFIRMATION";
}
