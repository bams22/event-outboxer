/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.relay.stream;

import org.springframework.messaging.Message;

/**
 * Broker delivery boundary. Return true only after the broker acknowledged the message; return
 * false or throw on rejection, timeout or an indeterminate outcome. Implementations must be
 * thread-safe and bound their wait. Buffer/channel acceptance alone is not an acknowledgement.
 */
@FunctionalInterface
public interface AcknowledgedStreamSender {
    boolean sendAndAwait(String binding, Message<?> message);
}
