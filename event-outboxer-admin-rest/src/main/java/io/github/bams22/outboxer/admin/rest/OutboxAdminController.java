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

import io.github.bams22.outboxer.admin.rest.AdminDtos.ArchivedEventResponse;
import io.github.bams22.outboxer.admin.rest.AdminDtos.CountResponse;
import io.github.bams22.outboxer.admin.rest.AdminDtos.EventPageResponse;
import io.github.bams22.outboxer.admin.rest.AdminDtos.EventResponse;
import io.github.bams22.outboxer.admin.rest.AdminDtos.PurgeArchiveRequest;
import io.github.bams22.outboxer.admin.rest.AdminDtos.PurgeDisabledRequest;
import io.github.bams22.outboxer.admin.rest.AdminDtos.ReenableAllRequest;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.OutboxAdmin;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.jspecify.annotations.Nullable;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Opt-in admin REST surface over {@link OutboxAdmin} (ADR-0019). Registered only when {@code
 * event-outboxer.admin.rest.enabled=true}. Every operation requires the authority named by {@code
 * event-outboxer.admin.rest.required-authority} — the {@code @PreAuthorize} SpEL reads it from the
 * {@code outboxAdminRestProperties} bean, so the permit is configuration, not code.
 *
 * <p>The class-level guard is enforced by Spring method security; the auto-configuration fail-fasts
 * at startup when Spring Security is present without {@code @EnableMethodSecurity} (see {@link
 * OutboxAdminRestAutoConfiguration}).
 */
@RestController
@RequestMapping("${event-outboxer.admin.rest.base-path:/outbox-admin}")
@PreAuthorize("hasAuthority(@outboxAdminRestProperties.getRequiredAuthority())")
public class OutboxAdminController {

  private final OutboxAdmin admin;
  private final EventStore store;

  public OutboxAdminController(OutboxAdmin admin, EventStore store) {
    this.admin = Objects.requireNonNull(admin, "admin must not be null");
    this.store = Objects.requireNonNull(store, "store must not be null");
  }

  /** Page of events; defaults to the DISABLED backlog, newest first. */
  @GetMapping("/events")
  public EventPageResponse events(
      @RequestParam(defaultValue = "DISABLED") EventStatus status,
      @RequestParam(required = false) @Nullable String type,
      @RequestParam(defaultValue = "50") int limit,
      @RequestParam(required = false) @Nullable String cursor) {
    List<Event> page = admin.findByStatus(status, type, limit, AdminDtos.decodeCursor(cursor));
    return EventPageResponse.of(page, limit);
  }

  /** Single event: the active table first, then the archive. */
  @GetMapping("/events/{id}")
  public ResponseEntity<Object> event(@PathVariable UUID id) {
    Optional<Event> active = store.findById(id);
    if (active.isPresent()) {
      return ResponseEntity.ok(EventResponse.from(active.get()));
    }
    return admin
        .findInArchive(id)
        .<Object>map(ArchivedEventResponse::from)
        .map(ResponseEntity::ok)
        .orElseGet(() -> ResponseEntity.notFound().build());
  }

  /**
   * Re-enable one DISABLED event: 200 on success, 404 for unknown ids, 409 when the event exists
   * but is not DISABLED.
   */
  @PostMapping("/events/{id}/reenable")
  public ResponseEntity<Object> reenable(@PathVariable UUID id) {
    if (admin.reenable(id)) {
      return ResponseEntity.ok(new CountResponse(1));
    }
    return store.findById(id).isPresent()
        ? ResponseEntity.status(409).body(new AdminDtos.ErrorResponse("event is not DISABLED"))
        : ResponseEntity.notFound().build();
  }

  /** Bulk re-enable for one event type. */
  @PostMapping("/events/reenable-all")
  public CountResponse reenableAll(@RequestBody ReenableAllRequest request) {
    return new CountResponse(
        admin.reenableAll(request.eventType(), request.createdBefore(), request.limitOrDefault()));
  }

  /** Delete old DISABLED rows. */
  @PostMapping("/purge/disabled")
  public CountResponse purgeDisabled(@RequestBody PurgeDisabledRequest request) {
    return new CountResponse(
        admin.purgeDisabled(request.eventType(), request.olderThan(), request.limitOrDefault()));
  }

  /** Delete old archive rows. */
  @PostMapping("/purge/archive")
  public CountResponse purgeArchive(@RequestBody PurgeArchiveRequest request) {
    return new CountResponse(
        admin.purgeArchive(request.archivedBefore(), request.limitOrDefault()));
  }
}
