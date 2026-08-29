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

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.domain.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Handler for {@link SendOrderConfirmationPayload}. In production this is where you would call
 * your SMTP gateway or message broker. The body is deliberately idempotent: re-sending the same
 * order confirmation is safe (the receiver dedupes by {@code orderId}).
 */
@Component
public class SendOrderConfirmationHandler implements EventHandler<SendOrderConfirmationPayload> {

  private static final Logger log = LoggerFactory.getLogger(SendOrderConfirmationHandler.class);

  @Override
  public EventType<SendOrderConfirmationPayload> type() {
    return SendOrderConfirmationPayload.EVENT_TYPE; // shared with OrderService.publish(...)
  }

  /**
   * Serialize orders to the same customer: concurrent confirmations for the same address should
   * not race. Returning {@code null} would disable locking for this handler.
   */
  @Override
  public String extractLockKey(SendOrderConfirmationPayload payload) {
    return "customer:" + payload.email();
  }

  @Override
  public EventOutcome handle(EventContext ctx, SendOrderConfirmationPayload payload) {
    log.info(
        "sending confirmation for order {} to {} (attempt {})",
        payload.orderId(),
        payload.email(),
        ctx.attempt());
    // In a real app: call SMTP / message broker / CRM.
    // Throw on transient errors; return Fail for permanent ones.
    return EventOutcome.success();
  }
}
