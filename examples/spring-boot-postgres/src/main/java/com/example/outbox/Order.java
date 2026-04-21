package com.example.outbox;

import java.time.Instant;
import java.util.UUID;

/** Business aggregate — stored in the {@code orders} table. */
public record Order(UUID id, String customerId, String email, long totalCents, Instant createdAt) {}
