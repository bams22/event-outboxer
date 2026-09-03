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
import io.github.bams22.outboxer.admin.rest.AdminDtos.ErrorResponse;
import io.github.bams22.outboxer.admin.rest.AdminDtos.EventPageResponse;
import io.github.bams22.outboxer.admin.rest.AdminDtos.EventResponse;
import io.github.bams22.outboxer.admin.rest.AdminDtos.PurgeArchiveRequest;
import io.github.bams22.outboxer.admin.rest.AdminDtos.PurgeDisabledRequest;
import io.github.bams22.outboxer.admin.rest.AdminDtos.ReenableAllRequest;
import io.github.bams22.outboxer.admin.rest.AdminDtos.ReplayAllRequest;
import io.github.bams22.outboxer.admin.rest.AdminDtos.ReplayAllResponse;
import io.github.bams22.outboxer.admin.rest.AdminDtos.ReplayResponse;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.OutboxAdmin;
import io.github.bams22.outboxer.spi.OutboxAdmin.ReplayAllResult;
import io.github.bams22.outboxer.spi.OutboxAdmin.ReplayOutcome;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.jspecify.annotations.Nullable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
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
        return admin.findInArchive(id)
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
                ? ResponseEntity.status(409)
                        .body(new AdminDtos.ErrorResponse("event is not DISABLED"))
                : ResponseEntity.notFound().build();
    }

    /** Bulk re-enable for one event type. */
    @PostMapping("/events/reenable-all")
    public CountResponse reenableAll(@RequestBody ReenableAllRequest request) {
        return new CountResponse(
                admin.reenableAll(
                        request.eventType(), request.createdBefore(), request.limitOrDefault()));
    }

    /**
     * Replay one archived event (ADR-0033): 200 with the outcome — {@code REPLAYED}, or {@code
     * COALESCED} when a PENDING event with the same {@code (event_type, dedup_key)} already exists
     * (nothing inserted, the archive row is kept) — 404 when the id is not in the archive, 409 when
     * the hot table already holds that id, which is a live event to look at rather than a replay.
     */
    @PostMapping("/events/{id}/replay")
    public ResponseEntity<Object> replay(@PathVariable UUID id) {
        ReplayOutcome outcome = admin.replayFromArchive(id);
        return switch (outcome) {
            case NOT_FOUND -> ResponseEntity.notFound().build();
            case ID_IN_USE ->
                    ResponseEntity.status(409)
                            .body(
                                    new AdminDtos.ErrorResponse(
                                            "an event with this id is already in the outbox"));
            case REPLAYED, COALESCED -> ResponseEntity.ok(new ReplayResponse(outcome.name()));
        };
    }

    /**
     * Bulk replay of archived events of one type, optionally bounded to an archived_at window.
     * Sweep by feeding {@code nextCursor} back as {@code cursor} until it comes back null — rows
     * that stayed archived are counted but never block the walk.
     */
    @PostMapping("/events/replay-all")
    public ReplayAllResponse replayAll(@RequestBody ReplayAllRequest request) {
        ReplayAllResult result =
                admin.replayAllFromArchive(
                        request.eventType(),
                        request.archivedAfter(),
                        request.archivedBefore(),
                        request.limitOrDefault(),
                        AdminDtos.decodeArchiveCursor(request.cursor()));
        return new ReplayAllResponse(
                result.replayed(),
                result.coalesced(),
                result.idInUse(),
                AdminDtos.encodeArchiveCursor(result.next()));
    }

    /** Delete old DISABLED rows. */
    @PostMapping("/purge/disabled")
    public CountResponse purgeDisabled(@RequestBody PurgeDisabledRequest request) {
        return new CountResponse(
                admin.purgeDisabled(
                        request.eventType(), request.olderThan(), request.limitOrDefault()));
    }

    /** Delete old archive rows. */
    @PostMapping("/purge/archive")
    public CountResponse purgeArchive(@RequestBody PurgeArchiveRequest request) {
        return new CountResponse(
                admin.purgeArchive(request.archivedBefore(), request.limitOrDefault()));
    }

    /**
     * A rejected argument is the caller's mistake, not a server fault: a malformed cursor, a
     * non-positive limit, a replay window whose bounds cannot enclose anything. The {@link
     * OutboxAdmin} port signals all of these with {@link IllegalArgumentException}, and without
     * this handler each would surface as a 500 with a stack trace — indistinguishable from the
     * store actually being broken, and unactionable for whoever wrote the request.
     *
     * <p>Scoped to this controller rather than a {@code @ControllerAdvice}: the admin surface is
     * opt-in and must not change how the host application renders its own exceptions.
     *
     * <p>The message is the port's own wording (it never contains request data beyond the values
     * the caller just sent), so echoing it is what makes the 400 useful.
     */
    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse badRequest(IllegalArgumentException ex) {
        return new ErrorResponse(ex.getMessage() == null ? "invalid request" : ex.getMessage());
    }
}
