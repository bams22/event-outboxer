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
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.spi.AdminCursor;
import io.github.bams22.outboxer.spi.ArchiveCursor;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTOs of the admin REST API. Thin records on purpose: the domain types stay free to evolve
 * without turning refactorings into breaking API changes.
 */
public final class AdminDtos {

    private AdminDtos() {}

    /**
     * One event of the active table. The dual payload lane (ADR-0025) maps to exactly one non-null
     * field: text payloads go out verbatim as {@code payload}, binary payloads as Base64 under
     * {@code payloadBase64}.
     */
    public record EventResponse(
            UUID id,
            String eventType,
            String status,
            int attempts,
            Instant createdAt,
            Instant runAt,
            @Nullable String lastFailReason,
            String payloadFormat,
            String payloadClass,
            @Nullable String payload,
            @Nullable String payloadBase64,
            @Nullable String dedupKey) {

        static EventResponse from(Event e) {
            return new EventResponse(
                    e.id(),
                    e.eventType(),
                    e.status().name(),
                    e.attempts(),
                    e.createdAt(),
                    e.runAt(),
                    e.lastFailReason(),
                    e.payloadFormat(),
                    e.payloadClass(),
                    e.payload().text(),
                    base64(e.payload()),
                    e.dedupKey());
        }
    }

    /** One archived event; payload lanes as in {@link EventResponse}. */
    public record ArchivedEventResponse(
            UUID id,
            String eventType,
            String status,
            int attempts,
            Instant createdAt,
            Instant archivedAt,
            String archivedBy,
            String payloadFormat,
            String payloadClass,
            @Nullable String payload,
            @Nullable String payloadBase64,
            @Nullable String dedupKey) {

        static ArchivedEventResponse from(ArchivedEvent e) {
            return new ArchivedEventResponse(
                    e.id(),
                    e.eventType(),
                    "ARCHIVED",
                    e.attempts(),
                    e.createdAt(),
                    e.archivedAt(),
                    e.archivedBy(),
                    e.payloadFormat(),
                    e.payloadClass(),
                    e.payload().text(),
                    base64(e.payload()),
                    e.dedupKey());
        }
    }

    private static @Nullable String base64(SerializedPayload payload) {
        byte[] bytes = payload.bytes();
        return bytes != null ? Base64.getEncoder().encodeToString(bytes) : null;
    }

    /** Keyset page; {@code nextCursor} is null on the last page. */
    public record EventPageResponse(List<EventResponse> events, @Nullable String nextCursor) {

        static EventPageResponse of(List<Event> page, int requestedLimit) {
            List<EventResponse> items = page.stream().map(EventResponse::from).toList();
            String next = page.size() < requestedLimit ? null : encodeCursor(page.getLast());
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

    /**
     * Bulk replay-from-archive request (ADR-0033); both window bounds are exclusive. {@code cursor}
     * continues a sweep — pass back the {@code nextCursor} of the previous response.
     */
    public record ReplayAllRequest(
            String eventType,
            @Nullable Instant archivedAfter,
            @Nullable Instant archivedBefore,
            @Nullable Integer limit,
            @Nullable String cursor) {

        int limitOrDefault() {
            return limit == null ? 100 : limit;
        }
    }

    /** Outcome of a single replay-from-archive: {@code REPLAYED} or {@code COALESCED}. */
    public record ReplayResponse(String outcome) {}

    /**
     * Counts of a bulk replay plus the cursor to continue from. Rows that stayed archived are
     * reported per reason — {@code coalesced} (a live PENDING event holds the same dedup key),
     * {@code idInUse} (the hot table already holds the id) — and neither stops the sweep: keep
     * calling with {@code nextCursor} until it comes back null.
     */
    public record ReplayAllResponse(
            int replayed, int coalesced, int idInUse, @Nullable String nextCursor) {}

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
                Instant.parse(cursor.substring(0, sep)),
                UUID.fromString(cursor.substring(sep + 1)));
    }

    static String encodeCursor(Event last) {
        return last.createdAt() + "_" + last.id();
    }

    /** Same wire shape as {@link #decodeCursor}, over the archive's {@code (archivedAt, id)}. */
    static @Nullable ArchiveCursor decodeArchiveCursor(@Nullable String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return null;
        }
        int sep = cursor.indexOf('_');
        if (sep <= 0) {
            throw new IllegalArgumentException("malformed cursor: " + cursor);
        }
        return new ArchiveCursor(
                Instant.parse(cursor.substring(0, sep)),
                UUID.fromString(cursor.substring(sep + 1)));
    }

    static @Nullable String encodeArchiveCursor(@Nullable ArchiveCursor cursor) {
        return cursor == null ? null : cursor.archivedAt() + "_" + cursor.id();
    }
}
