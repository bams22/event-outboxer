/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.target.outboxer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.bams22.outboxer.benchmark.db.DatabaseCoordinates;
import io.github.bams22.outboxer.benchmark.scenario.Scenario;
import io.github.bams22.outboxer.serializer.jackson.JacksonObjectMapperFactory;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Everything a forked worker JVM needs to know, handed over as a JSON file: where the database is,
 * what the scenario looks like, which id to take, and where to signal readiness.
 *
 * @param database benchmark database coordinates
 * @param redisUri Redis URI for the {@code redis} locker, {@code null} otherwise
 * @param scenario the effective scenario
 * @param workerId the id this process must run under
 * @param readyFile path the worker creates once its context is up
 */
public record WorkerSpec(
        DatabaseCoordinates database,
        @Nullable String redisUri,
        Scenario scenario,
        String workerId,
        String readyFile) {

    private static final ObjectMapper MAPPER = JacksonObjectMapperFactory.defaults();

    public WorkerSpec {
        Objects.requireNonNull(database, "database must not be null");
        Objects.requireNonNull(scenario, "scenario must not be null");
        Objects.requireNonNull(workerId, "workerId must not be null");
        Objects.requireNonNull(readyFile, "readyFile must not be null");
    }

    /** Writes this spec to {@code file}. */
    public void write(Path file) {
        try {
            MAPPER.writeValue(file.toFile(), this);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot write worker spec " + file, e);
        }
    }

    /** Reads a spec from {@code file}. */
    public static WorkerSpec read(Path file) {
        try {
            return MAPPER.readValue(file.toFile(), WorkerSpec.class);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read worker spec " + file, e);
        }
    }
}
