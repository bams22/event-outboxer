/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.storage.postgres;

import java.time.Duration;
import java.util.Objects;
import lombok.Builder;

/**
 * Plain POJO configuration for the PostgreSQL adapter. Kept free of Spring
 * {@code @ConfigurationProperties} so the adapter can be used from plain-Java setups too — the
 * Spring Boot starter (P9) binds YAML into this record.
 *
 * @param schema database schema holding the outbox tables; defaults to {@code event_outboxer} — a
 *     specific name chosen to avoid clashing with other libraries or application tables in a shared
 *     database
 * @param tablePrefix optional prefix applied before table names; defaults to empty. Useful when
 *     multiple outboxes share a schema.
 * @param archiveEnabled when {@code true}, {@code markProcessed} copies the row to {@code
 *     event_archive} before deleting from {@code events}. The archive migration {@code V002} must
 *     have been applied.
 * @param metricsCacheTtl TTL of the in-memory cache for {@link
 *     io.github.bams22.outboxer.spi.EventStore#metricsSnapshot()}; short so dashboards stay fresh
 *     but long enough not to hammer the DB.
 */
@Builder
public record PostgresStorageProperties(
        String schema, String tablePrefix, boolean archiveEnabled, Duration metricsCacheTtl) {

    public PostgresStorageProperties {
        Objects.requireNonNull(schema, "schema must not be null");
        if (schema.isBlank()) {
            throw new IllegalArgumentException("schema must not be blank");
        }
        Objects.requireNonNull(tablePrefix, "tablePrefix must not be null");
        Objects.requireNonNull(metricsCacheTtl, "metricsCacheTtl must not be null");
        if (metricsCacheTtl.isNegative()) {
            throw new IllegalArgumentException(
                    "metricsCacheTtl must not be negative, got " + metricsCacheTtl);
        }
    }

    /** Canonical defaults. */
    public static PostgresStorageProperties defaults() {
        return PostgresStorageProperties.builder()
                .schema("event_outboxer")
                .tablePrefix("")
                .archiveEnabled(false)
                .metricsCacheTtl(Duration.ofSeconds(30))
                .build();
    }
}
