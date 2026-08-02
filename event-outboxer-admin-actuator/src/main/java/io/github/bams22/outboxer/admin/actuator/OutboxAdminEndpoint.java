/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.admin.actuator;

import io.github.bams22.outboxer.domain.ArchivedEvent;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.spi.AdminCursor;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.OutboxAdmin;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.actuate.endpoint.annotation.DeleteOperation;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;

/**
 * Actuator surface over {@link OutboxAdmin} (ADR-0019). Endpoint id {@code outboxadmin} — the id
 * {@code outbox} is taken by the health indicator. Not exposed by default; enable with {@code
 * management.endpoints.web.exposure.include=outboxadmin} and secure it the same way as every other
 * Actuator endpoint.
 *
 * <p>Operations map to:
 *
 * <pre>
 * GET    /actuator/outboxadmin?status=DISABLED&amp;eventType=X&amp;limit=50&amp;cursor=...
 * GET    /actuator/outboxadmin/{id}
 * POST   /actuator/outboxadmin/{id}            {"action": "reenable"}
 * POST   /actuator/outboxadmin                 {"eventType": "X", "limit": 100}   (reenable-all)
 * DELETE /actuator/outboxadmin?target=disabled&amp;olderThanDays=90&amp;limit=1000
 * </pre>
 */
@Endpoint(id = "outboxadmin")
public class OutboxAdminEndpoint {

  private final OutboxAdmin admin;
  private final EventStore store;

  public OutboxAdminEndpoint(OutboxAdmin admin, EventStore store) {
    this.admin = Objects.requireNonNull(admin, "admin must not be null");
    this.store = Objects.requireNonNull(store, "store must not be null");
  }

  /**
   * Page of events; defaults to the DISABLED backlog, newest first.
   *
   * <p>Optional parameters carry {@code org.springframework.lang.Nullable} in addition to the
   * JSpecify annotation: Actuator's {@code OperationMethodParameter.isMandatory()} only recognizes
   * the Spring (and JSR-305) annotations, so without it every parameter would be treated as
   * mandatory and requests omitting one would fail with HTTP 400.
   */
  @ReadOperation
  public Map<String, @Nullable Object> events(
      @org.springframework.lang.Nullable @Nullable String status,
      @org.springframework.lang.Nullable @Nullable String eventType,
      @org.springframework.lang.Nullable @Nullable Integer limit,
      @org.springframework.lang.Nullable @Nullable String cursor) {
    EventStatus resolvedStatus =
        status == null ? EventStatus.DISABLED : EventStatus.valueOf(status.toUpperCase());
    int resolvedLimit = limit == null ? 50 : limit;
    List<Event> page =
        admin.findByStatus(resolvedStatus, eventType, resolvedLimit, CursorCodec.decode(cursor));
    Map<String, @Nullable Object> body = new LinkedHashMap<>();
    body.put("events", page.stream().map(OutboxAdminEndpoint::toMap).toList());
    body.put("nextCursor", page.size() < resolvedLimit ? null : CursorCodec.encode(page.getLast()));
    return body;
  }

  /** Single event by id: the active table first, then the archive. */
  @ReadOperation
  public @Nullable Map<String, @Nullable Object> event(@Selector String id) {
    UUID uuid = UUID.fromString(id);
    Optional<Event> active = store.findById(uuid);
    if (active.isPresent()) {
      return toMap(active.get());
    }
    return admin.findInArchive(uuid).map(OutboxAdminEndpoint::toArchivedMap).orElse(null);
  }

  /** Re-enable one DISABLED event. */
  @WriteOperation
  public Map<String, Object> reenable(@Selector String id) {
    boolean reenabled = admin.reenable(UUID.fromString(id));
    return Map.of("reenabled", reenabled);
  }

  /** Bulk re-enable for one event type. */
  @WriteOperation
  public Map<String, Object> reenableAll(
      String eventType, @org.springframework.lang.Nullable @Nullable Integer limit) {
    int count = admin.reenableAll(eventType, null, limit == null ? 100 : limit);
    return Map.of("reenabled", count);
  }

  /** Purge old DISABLED rows ({@code target=disabled}) or archive rows ({@code target=archive}). */
  @DeleteOperation
  public Map<String, Object> purge(
      String target,
      long olderThanDays,
      @org.springframework.lang.Nullable @Nullable String eventType,
      @org.springframework.lang.Nullable @Nullable Integer limit) {
    Instant threshold = Instant.now().minus(Duration.ofDays(olderThanDays));
    int resolvedLimit = limit == null ? 1000 : limit;
    int purged =
        switch (target) {
          case "disabled" -> admin.purgeDisabled(eventType, threshold, resolvedLimit);
          case "archive" -> admin.purgeArchive(threshold, resolvedLimit);
          default ->
              throw new IllegalArgumentException(
                  "target must be 'disabled' or 'archive', got '" + target + "'");
        };
    return Map.of("purged", purged);
  }

  // ---------------------------------------------------------------------------------------------
  // mapping
  // ---------------------------------------------------------------------------------------------

  private static Map<String, @Nullable Object> toMap(Event e) {
    Map<String, @Nullable Object> m = new LinkedHashMap<>();
    m.put("id", e.id().toString());
    m.put("eventType", e.eventType());
    m.put("status", e.status().name());
    m.put("attempts", e.attempts());
    m.put("createdAt", e.createdAt().toString());
    m.put("runAt", e.runAt().toString());
    m.put("lastFailReason", e.lastFailReason());
    m.put("payloadFormat", e.payloadFormat());
    m.put("payloadClass", e.payloadClass());
    putPayload(m, e.payload());
    return m;
  }

  private static Map<String, @Nullable Object> toArchivedMap(ArchivedEvent e) {
    Map<String, @Nullable Object> m = new LinkedHashMap<>();
    m.put("id", e.id().toString());
    m.put("eventType", e.eventType());
    m.put("status", "ARCHIVED");
    m.put("attempts", e.attempts());
    m.put("createdAt", e.createdAt().toString());
    m.put("archivedAt", e.archivedAt().toString());
    m.put("archivedBy", e.archivedBy());
    m.put("payloadFormat", e.payloadFormat());
    m.put("payloadClass", e.payloadClass());
    putPayload(m, e.payload());
    return m;
  }

  /**
   * Dual payload lane (ADR-0025): the text lane goes out verbatim as {@code payload}, the binary
   * lane as Base64 under {@code payloadBase64} — exactly one of the two keys is non-null.
   */
  private static void putPayload(Map<String, @Nullable Object> m, SerializedPayload payload) {
    m.put("payload", payload.text());
    byte[] bytes = payload.bytes();
    m.put("payloadBase64", bytes != null ? Base64.getEncoder().encodeToString(bytes) : null);
  }

  /**
   * Opaque page cursor: {@code <iso-instant>_<uuid>} of the last row of the previous page. Full ISO
   * precision on purpose — truncating to millis would make the strict keyset comparison skip rows
   * that share a millisecond with the cursor row (PG timestamps are microsecond).
   */
  static final class CursorCodec {

    private CursorCodec() {}

    static @Nullable AdminCursor decode(@Nullable String cursor) {
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

    static String encode(Event last) {
      return last.createdAt() + "_" + last.id();
    }
  }
}
