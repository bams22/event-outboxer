/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.maintenance;

import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.observer.StuckHandlerReclaimedInfo;
import io.github.bams22.outboxer.core.config.EventTypeConfig;
import io.github.bams22.outboxer.core.config.EventTypeConfigProvider;
import io.github.bams22.outboxer.core.dispatch.InFlightRegistry;
import io.github.bams22.outboxer.core.dispatch.InFlightRegistry.Entry;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EventStore;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scans the {@link InFlightRegistry} every tick and force-reclaims entries whose handler has been
 * running longer than the per-type {@code handlerMaxRuntime}. Uses {@code
 * EventStore.forceReclaim(...)}; the optimistic-lock check inside the storage ensures that a
 * finalize from a slow-but-eventually-successful handler still loses the race cleanly (see
 * ADR-0014).
 */
public final class WatchdogTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(WatchdogTask.class);

    private final InFlightRegistry inFlight;
    private final EventStore store;
    private final Clock clock;
    private final EventTypeConfigProvider typeConfig;
    private final OutboxListener listener;

    public WatchdogTask(
            InFlightRegistry inFlight,
            EventStore store,
            Clock clock,
            EventTypeConfigProvider typeConfig,
            OutboxListener listener) {
        this.inFlight = Objects.requireNonNull(inFlight);
        this.store = Objects.requireNonNull(store);
        this.clock = Objects.requireNonNull(clock);
        this.typeConfig = Objects.requireNonNull(typeConfig);
        this.listener = Objects.requireNonNull(listener);
    }

    @Override
    public void run() {
        Instant now = clock.now();
        for (Entry entry : inFlight.snapshot()) {
            try {
                EventTypeConfig cfg = typeConfig.forType(entry.eventType());
                Duration elapsed = Duration.between(entry.startedAt(), now);
                if (elapsed.compareTo(cfg.handlerMaxRuntime()) <= 0) {
                    continue;
                }
                boolean reclaimed =
                        store.forceReclaim(
                                entry.eventId(), entry.workerId(), entry.claimedVersion(), now);
                if (reclaimed) {
                    inFlight.unregister(entry.eventId());
                    listener.onStuckHandlerReclaimed(
                            new StuckHandlerReclaimedInfo(
                                    entry.eventId(), entry.eventType(), elapsed, entry.workerId()));
                    log.warn(
                            "watchdog reclaimed stuck handler: eventId={} type={} elapsed={}",
                            entry.eventId(),
                            entry.eventType(),
                            elapsed);
                }
            } catch (RuntimeException ex) {
                log.warn(
                        "watchdog force-reclaim failed for eventId={}: {}",
                        entry.eventId(),
                        ex.toString());
            }
        }
    }
}
