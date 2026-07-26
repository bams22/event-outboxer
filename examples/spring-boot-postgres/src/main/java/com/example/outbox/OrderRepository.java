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
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class OrderRepository {

  private final JdbcTemplate jdbc;

  public OrderRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  public void save(Order order) {
    jdbc.update(
        "INSERT INTO orders(id, customer_id, email, total_cents, created_at) VALUES (?, ?, ?, ?, ?)",
        order.id(),
        order.customerId(),
        order.email(),
        order.totalCents(),
        java.sql.Timestamp.from(order.createdAt()));
  }

  public Optional<Order> findById(UUID id) {
    return jdbc
        .query(
            "SELECT id, customer_id, email, total_cents, created_at FROM orders WHERE id = ?",
            (rs, rowNum) ->
                new Order(
                    (UUID) rs.getObject("id"),
                    rs.getString("customer_id"),
                    rs.getString("email"),
                    rs.getLong("total_cents"),
                    rs.getTimestamp("created_at").toInstant()),
            id)
        .stream()
        .findFirst();
  }

  public long count() {
    Long c = jdbc.queryForObject("SELECT count(*) FROM orders", Long.class);
    return c == null ? 0L : c;
  }

  public Instant now() {
    return Instant.now();
  }
}
