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

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import io.github.bams22.outboxer.benchmark.scenario.PostgresRestart;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Owns the benchmark database for the duration of a run: either a disposable Testcontainers
 * PostgreSQL (convenient, never the source of published numbers) or an external instance the
 * operator points at. Only the disposable one can be restarted under the fleet.
 */
public interface DatabaseHandle extends AutoCloseable {

    /** How long {@link #restart} waits for the server to answer again before giving up. */
    Duration RESTART_TIMEOUT = Duration.ofSeconds(60);

    /** Where to connect. */
    DatabaseCoordinates coordinates();

    /** Human-readable origin for the report: {@code testcontainers:<image>} or {@code external}. */
    String origin();

    /** Whether {@link #restart} is possible: {@code true} only for the disposable container. */
    boolean supportsRestart();

    /**
     * Takes the server down with the mode's signal, starts it again on the same address, and
     * returns once it accepts connections. Same container, same data directory, same host port.
     *
     * @return how long the database was unreachable, from signal to first successful connection
     * @throws UnsupportedOperationException for an external database
     */
    Duration restart(PostgresRestart mode);

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
            public boolean supportsRestart() {
                return false;
            }

            @Override
            public Duration restart(PostgresRestart mode) {
                throw new UnsupportedOperationException(
                        "PostgreSQL restart needs the disposable Testcontainers database; drop"
                                + " --bench.jdbc-url or set --bench.pg-restart=none");
            }

            @Override
            public void close() {}
        };
    }

    /**
     * Starts a disposable PostgreSQL container from the given image and stops it on close. The host
     * port is fixed at creation (a free ephemeral port picked here) rather than assigned by Docker
     * at start, so a restart of the container keeps the address every worker holds.
     */
    static DatabaseHandle testcontainers(String image) {
        Objects.requireNonNull(image, "image must not be null");
        int hostPort = freePort();
        PostgreSQLContainer<?> container =
                new PostgreSQLContainer<>(image)
                        .withDatabaseName("bench")
                        .withUsername("bench")
                        .withPassword("bench")
                        .withCreateContainerCmdModifier(
                                cmd ->
                                        Objects.requireNonNull(cmd.getHostConfig())
                                                .withPortBindings(
                                                        new PortBinding(
                                                                Ports.Binding.bindPort(hostPort),
                                                                ExposedPort.tcp(
                                                                        PostgreSQLContainer
                                                                                .POSTGRESQL_PORT))));
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
            public boolean supportsRestart() {
                return true;
            }

            @Override
            public Duration restart(PostgresRestart mode) {
                if (mode == PostgresRestart.NONE) {
                    throw new IllegalArgumentException("restart mode must not be NONE");
                }
                DockerClient docker = container.getDockerClient();
                String id = container.getContainerId();
                Instant down = Instant.now();
                docker.killContainerCmd(id).withSignal(mode.signal()).exec();
                waitUntilStopped(docker, id);
                docker.startContainerCmd(id).exec();
                new PgProbe(coordinates).awaitReady(RESTART_TIMEOUT);
                return Duration.between(down, Instant.now());
            }

            @Override
            public void close() {
                container.stop();
            }
        };
    }

    private static void waitUntilStopped(DockerClient docker, String id) {
        Instant deadline = Instant.now().plus(RESTART_TIMEOUT);
        while (Boolean.TRUE.equals(docker.inspectContainerCmd(id).exec().getState().getRunning())) {
            if (Instant.now().isAfter(deadline)) {
                throw new IllegalStateException("PostgreSQL container did not stop in time");
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for the stop", e);
            }
        }
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot pick a free host port", e);
        }
    }
}
