/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.storage.postgres.internal;

import io.github.bams22.outboxer.domain.ArchivedEvent;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.WorkerId;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jspecify.annotations.Nullable;

/**
 * The column lists of the {@code events} and {@code event_archive} tables and the mappers that read
 * them, kept together so that a column is added in one place. {@code PostgresEventStore} and {@code
 * PostgresOutboxAdmin} both build their statements from these constants — the publish INSERT, the
 * lookups, the archive copy on finalize and on the claim's sweep, the replay — and read the rows
 * back with the mappers next to them. Before this class each of those statements carried its own
 * copy of the list, and a column added to the store (as ADR-0037's {@code dedup_key} was) reached
 * the admin only if someone remembered.
 *
 * <p>The lists are split by lifecycle, not by table. {@link #IMMUTABLE_COLUMNS} are the columns set
 * at publish and never updated afterwards — identity, payload, priority, trace context, dedup key —
 * and they are what every statement copies around. Every other column is lifecycle state whose
 * value differs per statement (a finalize copies {@code attempts}, a replay resets it), so those
 * are spelled out where they are used.
 */
public final class EventRows {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /**
     * Columns set at publish and never updated: copied verbatim to the archive on finalize and back
     * to the hot table on replay. Order matters wherever the list feeds a {@code VALUES} or a
     * {@code SELECT} source — every user of this constant binds or selects in exactly this order.
     *
     * <p>What adding a column here does and does not catch. The statements that build both sides
     * from this constant (the archive copies, the replay) grow together, so PostgreSQL stays silent
     * unless the target table lacks the column; the mappers next to this class simply ignore a
     * column they do not read. Only {@code PostgresEventStore.sqlInsert} fails loudly, because its
     * {@code VALUES} placeholders are hand-written and the lengths then disagree. So a new column
     * is a four-step change — the constant, the {@code VALUES} list and its binder, a migration on
     * both tables, and the mapper — and {@code EventRowsTest} pins the column count of each list as
     * a tripwire, while {@code EventRowsSchemaIT} checks the lists against the migrated schema.
     */
    public static final String IMMUTABLE_COLUMNS =
            "id, event_type, payload, payload_binary, payload_format, payload_class, priority,"
                    + " trace_context, dedup_key";

    /**
     * Every column of an {@code events} row — the shape {@link #readEvent(ResultSet)} expects.
     */
    public static final String EVENT_COLUMNS =
            IMMUTABLE_COLUMNS
                    + ", attempts, status, created_at, run_at, claimed_by, claimed_at,"
                    + " last_fail_reason, version";

    /**
     * The part of an {@code events} row the archive keeps: {@link #IMMUTABLE_COLUMNS} plus the
     * lifecycle state worth auditing. Both archive INSERTs select exactly this list from the row
     * they delete, followed by {@code archived_at} and {@code archived_by}.
     */
    public static final String ARCHIVE_COPY_COLUMNS =
            IMMUTABLE_COLUMNS + ", attempts, created_at, run_at, last_fail_reason";

    /**
     * Every column of an {@code event_archive} row — the target list of both archive INSERTs and
     * the shape {@link #readArchived(ResultSet)} expects.
     */
    public static final String ARCHIVE_COLUMNS =
            ARCHIVE_COPY_COLUMNS + ", archived_at, archived_by";

    private EventRows() {}

    /**
     * The column list with every name qualified by a table alias: {@code qualified("e", "id,
     * dedup_key")} is {@code "e.id, e.dedup_key"}. For statements that join the row to other
     * relations and must disambiguate.
     *
     * <p>Only lists of plain column identifiers are accepted. Prefixing an expression, a cast or an
     * alias with the table alias would silently produce malformed SQL ({@code "a::text, f(b, c)"}
     * splits on the wrong commas), so a segment that is not an identifier is rejected here rather
     * than at the database.
     */
    public static String qualified(String alias, String columns) {
        Objects.requireNonNull(alias, "alias must not be null");
        Objects.requireNonNull(columns, "columns must not be null");
        return Stream.of(columns.split("\\s*,\\s*"))
                .map(column -> alias + "." + requireIdentifier(column, columns))
                .collect(Collectors.joining(", "));
    }

    private static String requireIdentifier(String column, String list) {
        if (!IDENTIFIER.matcher(column).matches()) {
            throw new IllegalArgumentException(
                    "qualified() takes plain column identifiers, got '"
                            + column
                            + "' in \""
                            + list
                            + "\"");
        }
        return column;
    }

    /**
     * Maps a row selected with {@link #EVENT_COLUMNS}.
     */
    public static Event readEvent(ResultSet rs) throws SQLException {
        String claimedBy = rs.getString("claimed_by");
        return new Event(
                (UUID) rs.getObject("id"),
                rs.getString("event_type"),
                readPayload(rs),
                rs.getString("payload_format"),
                rs.getString("payload_class"),
                rs.getShort("priority"),
                rs.getInt("attempts"),
                EventStatus.valueOf(rs.getString("status")),
                instant(rs.getTimestamp("created_at")),
                instant(rs.getTimestamp("run_at")),
                claimedBy == null ? null : new WorkerId(claimedBy),
                instantOrNull(rs.getTimestamp("claimed_at")),
                rs.getString("last_fail_reason"),
                traceContext(rs.getString("trace_context")),
                rs.getLong("version"),
                rs.getString("dedup_key"));
    }

    /**
     * Maps a row selected with {@link #ARCHIVE_COLUMNS}.
     */
    public static ArchivedEvent readArchived(ResultSet rs) throws SQLException {
        return new ArchivedEvent(
                (UUID) rs.getObject("id"),
                rs.getString("event_type"),
                readPayload(rs),
                rs.getString("payload_format"),
                rs.getString("payload_class"),
                rs.getShort("priority"),
                rs.getInt("attempts"),
                instant(rs.getTimestamp("created_at")),
                instant(rs.getTimestamp("run_at")),
                rs.getString("last_fail_reason"),
                traceContext(rs.getString("trace_context")),
                instant(rs.getTimestamp("archived_at")),
                rs.getString("archived_by"),
                rs.getString("dedup_key"));
    }

    /**
     * Reassembles the dual payload lane (ADR-0025) from {@code payload} / {@code payload_binary};
     * the CHECK constraint guarantees exactly one of the two is non-null.
     */
    public static SerializedPayload readPayload(ResultSet rs) throws SQLException {
        byte[] binary = rs.getBytes("payload_binary");
        return binary != null
                ? SerializedPayload.ofBytes(binary)
                : SerializedPayload.ofText(rs.getString("payload"));
    }

    /**
     * The stored {@code trace_context} JSON as a flat map; an absent or empty column is an empty
     * map, never {@code null}.
     */
    public static Map<String, String> traceContext(@Nullable String json) {
        if (json == null || json.isEmpty()) {
            return Map.of();
        }
        return FlatMapJson.parse(json);
    }

    /**
     * A NOT NULL timestamp column as an {@link Instant}.
     */
    public static Instant instant(Timestamp ts) {
        return ts.toInstant();
    }

    /**
     * A nullable timestamp column as an {@link Instant}, {@code null} preserved.
     */
    public static @Nullable Instant instantOrNull(@Nullable Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }
}
