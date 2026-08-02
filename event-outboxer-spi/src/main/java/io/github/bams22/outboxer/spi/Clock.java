/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spi;

import java.time.Instant;

/**
 * Time abstraction used throughout the engine instead of {@code java.time.Instant.now()} so that
 * tests (via {@code testkit.SettableClock}) can advance virtual time deterministically.
 *
 * <p>Functional interface — production code wires {@link #system()} or an equivalent {@code Clock c
 * = Instant::now} lambda. Adapters must never call {@link Instant#now()} directly if the behaviour
 * depends on wall-clock (claim eligibility, retry {@code runAt}, heartbeat staleness); those sites
 * must go through this port.
 */
@FunctionalInterface
public interface Clock {

    /** Current wall-clock time. Implementations must be thread-safe. */
    Instant now();

    /**
     * System clock backed by {@link Instant#now()}. Suitable for production wiring. Returns a fresh
     * instance each call because the contract is trivially stateless; callers may cache the result.
     */
    static Clock system() {
        return Instant::now;
    }
}
