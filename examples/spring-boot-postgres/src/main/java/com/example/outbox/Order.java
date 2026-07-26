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

import java.time.Instant;
import java.util.UUID;

/** Business aggregate — stored in the {@code orders} table. */
public record Order(UUID id, String customerId, String email, long totalCents, Instant createdAt) {}
