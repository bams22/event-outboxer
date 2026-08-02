/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.testkit;

import io.github.bams22.outboxer.spi.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Mutable {@link Clock} for deterministic tests. Wrap it around the outbox and then advance time by
 * fixed increments between dispatcher ticks to exercise retry scheduling, watchdog timeouts and
 * orphan recovery without wall-clock waits.
 *
 * <p>Thread-safe — reads and updates go through an {@link AtomicReference}.
 */
public final class SettableClock implements Clock {

    private final AtomicReference<Instant> current;

    public SettableClock(Instant initial) {
        this.current =
                new AtomicReference<>(Objects.requireNonNull(initial, "initial must not be null"));
    }

    /** Fixed at the current system time. Equivalent to {@code new SettableClock(Instant.now())}. */
    public static SettableClock atSystemNow() {
        return new SettableClock(Instant.now());
    }

    /** Fixed at {@link Instant#EPOCH}. Useful when tests want reproducible absolute times. */
    public static SettableClock atEpoch() {
        return new SettableClock(Instant.EPOCH);
    }

    @Override
    public Instant now() {
        return current.get();
    }

    /** Move the clock forward by {@code duration}; negative values move it back. */
    public Instant advance(Duration duration) {
        Objects.requireNonNull(duration, "duration must not be null");
        return current.updateAndGet(prev -> prev.plus(duration));
    }

    /** Reset the clock to {@code newNow}. */
    public Instant set(Instant newNow) {
        Objects.requireNonNull(newNow, "newNow must not be null");
        current.set(newNow);
        return newNow;
    }
}
