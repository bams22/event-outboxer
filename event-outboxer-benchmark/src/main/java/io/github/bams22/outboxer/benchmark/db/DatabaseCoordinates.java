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

/**
 * JDBC coordinates of the benchmark database.
 *
 * @param jdbcUrl {@code jdbc:postgresql://...}
 * @param username login role
 * @param password its password
 */
public record DatabaseCoordinates(String jdbcUrl, String username, String password) {

    public DatabaseCoordinates {
        Objects.requireNonNull(jdbcUrl, "jdbcUrl must not be null");
        Objects.requireNonNull(username, "username must not be null");
        Objects.requireNonNull(password, "password must not be null");
        if (!jdbcUrl.startsWith("jdbc:postgresql:")) {
            throw new IllegalArgumentException(
                    "The harness runs on PostgreSQL only (ADR-0020, ADR-0034); got " + jdbcUrl);
        }
    }
}
