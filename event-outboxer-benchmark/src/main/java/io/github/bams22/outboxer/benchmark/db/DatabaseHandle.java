/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.db;

import java.util.Objects;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Owns the benchmark database for the duration of a run: either a disposable Testcontainers
 * PostgreSQL (convenient, never the source of published numbers) or an external instance the
 * operator points at.
 */
public interface DatabaseHandle extends AutoCloseable {

    /** Where to connect. */
    DatabaseCoordinates coordinates();

    /** Human-readable origin for the report: {@code testcontainers:<image>} or {@code external}. */
    String origin();

    @Override
    void close();

    /** An existing database; {@link #close()} is a no-op. */
    static DatabaseHandle external(DatabaseCoordinates coordinates) {
        Objects.requireNonNull(coordinates, "coordinates must not be null");
        return new DatabaseHandle() {
            @Override
            public DatabaseCoordinates coordinates() {
                return coordinates;
            }

            @Override
            public String origin() {
                return "external";
            }

            @Override
            public void close() {}
        };
    }

    /** Starts a disposable PostgreSQL container from the given image and stops it on close. */
    static DatabaseHandle testcontainers(String image) {
        Objects.requireNonNull(image, "image must not be null");
        PostgreSQLContainer<?> container =
                new PostgreSQLContainer<>(image)
                        .withDatabaseName("bench")
                        .withUsername("bench")
                        .withPassword("bench");
        container.start();
        DatabaseCoordinates coordinates =
                new DatabaseCoordinates(
                        container.getJdbcUrl(), container.getUsername(), container.getPassword());
        return new DatabaseHandle() {
            @Override
            public DatabaseCoordinates coordinates() {
                return coordinates;
            }

            @Override
            public String origin() {
                return "testcontainers:" + image;
            }

            @Override
            public void close() {
                container.stop();
            }
        };
    }
}
