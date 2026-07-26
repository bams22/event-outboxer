/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.admin.rest;

import io.github.bams22.outboxer.domain.ArchivedEvent;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.spi.AdminCursor;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTOs of the admin REST API. Thin records on purpose: the domain types stay free to evolve
 * without turning refactorings into breaking API changes.
 */
public final class AdminDtos {

  private AdminDtos() {}

  /** One event of the active table. */
  public record EventResponse(
      UUID id,
      String eventType,
      String status,
      int attempts,
      Instant createdAt,
      Instant runAt,
      @Nullable String lastFailReason,
      String payloadClass,
      String payload) {

    static EventResponse from(Event e) {
      return new EventResponse(
          e.id(),
          e.eventType(),
          e.status().name(),
          e.attempts(),
          e.createdAt(),
          e.runAt(),
          e.lastFailReason(),
          e.payloadClass(),
          e.payload());
    }
  }

  /** One archived event. */
  public record ArchivedEventResponse(
      UUID id,
      String eventType,
      String status,
      int attempts,
      Instant createdAt,
      Instant archivedAt,
      String archivedBy,
      String payloadClass,
      String payload) {

    static ArchivedEventResponse from(ArchivedEvent e) {
      return new ArchivedEventResponse(
          e.id(),
          e.eventType(),
          "ARCHIVED",
          e.attempts(),
          e.createdAt(),
          e.archivedAt(),
          e.archivedBy(),
          e.payloadClass(),
          e.payload());
    }
  }

  /** Keyset page; {@code nextCursor} is null on the last page. */
  public record EventPageResponse(List<EventResponse> events, @Nullable String nextCursor) {

    static EventPageResponse of(List<Event> page, int requestedLimit) {
      List<EventResponse> items = page.stream().map(EventResponse::from).toList();
      String next =
          page.size() < requestedLimit ? null : encodeCursor(page.get(page.size() - 1));
      return new EventPageResponse(items, next);
    }
  }

  /** Bulk re-enable request. */
  public record ReenableAllRequest(
      String eventType, @Nullable Instant createdBefore, @Nullable Integer limit) {

    int limitOrDefault() {
      return limit == null ? 100 : limit;
    }
  }

  /** Purge request for DISABLED rows. */
  public record PurgeDisabledRequest(
      Instant olderThan, @Nullable String eventType, @Nullable Integer limit) {

    int limitOrDefault() {
      return limit == null ? 1000 : limit;
    }
  }

  /** Purge request for archive rows. */
  public record PurgeArchiveRequest(Instant archivedBefore, @Nullable Integer limit) {

    int limitOrDefault() {
      return limit == null ? 1000 : limit;
    }
  }

  /** Row-count result of a bulk operation. */
  public record CountResponse(int count) {}

  /** Machine-readable error body. */
  public record ErrorResponse(String error) {}

  /**
   * Opaque page cursor: {@code <iso-instant>_<uuid>} of the last row of the previous page. Full
   * ISO precision — millisecond truncation would make the strict keyset comparison skip rows.
   */
  static @Nullable AdminCursor decodeCursor(@Nullable String cursor) {
    if (cursor == null || cursor.isBlank()) {
      return null;
    }
    int sep = cursor.indexOf('_');
    if (sep <= 0) {
      throw new IllegalArgumentException("malformed cursor: " + cursor);
    }
    return new AdminCursor(
        Instant.parse(cursor.substring(0, sep)), UUID.fromString(cursor.substring(sep + 1)));
  }

  static String encodeCursor(Event last) {
    return last.createdAt() + "_" + last.id();
  }
}
