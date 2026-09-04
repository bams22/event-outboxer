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
import org.testcontainers.containers.GenericContainer;

/**
 * Owns the Redis/KeyDB the {@code redis} locker talks to for the duration of a run: a disposable
 * Testcontainers instance or an external one the operator points at. Opened only when the
 * scenario's lock type is {@code redis}.
 */
public interface RedisHandle extends AutoCloseable {

    /** Lettuce URI, {@code redis://host:port}. */
    String uri();

    /** {@code external} or {@code testcontainers:<image>}. */
    String origin();

    @Override
    void close();

    /** An existing server; {@link #close()} is a no-op. */
    static RedisHandle external(String uri) {
        Objects.requireNonNull(uri, "uri must not be null");
        return new RedisHandle() {
            @Override
            public String uri() {
                return uri;
            }

            @Override
            public String origin() {
                return "external";
            }

            @Override
            public void close() {}
        };
    }

    /** Starts a disposable container from {@code image} and stops it on close. */
    static RedisHandle testcontainers(String image) {
        Objects.requireNonNull(image, "image must not be null");
        GenericContainer<?> container = new GenericContainer<>(image).withExposedPorts(6379);
        container.start();
        String uri = "redis://" + container.getHost() + ":" + container.getMappedPort(6379);
        return new RedisHandle() {
            @Override
            public String uri() {
                return uri;
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
