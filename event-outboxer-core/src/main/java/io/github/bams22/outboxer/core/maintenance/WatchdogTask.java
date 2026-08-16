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

import io.github.bams22.outboxer.api.observer.HandlerAbandonedInfo;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.observer.StuckHandlerReclaimedInfo;
import io.github.bams22.outboxer.core.config.EventTypeConfig;
import io.github.bams22.outboxer.core.config.EventTypeConfigProvider;
import io.github.bams22.outboxer.core.dispatch.InFlightRegistry;
import io.github.bams22.outboxer.core.dispatch.InFlightRegistry.Abandoned;
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
 *
 * <p>Because that finalize is guaranteed to lose, letting the handler run on is pure waste: unless
 * the type opts out via {@code interruptStuckHandler}, the dispatching thread is interrupted so
 * anything blocked in an interruptible call unwinds and returns its slot to the handler pool.
 * Nothing can force a thread that ignores interrupts (blocking I/O with no timeout is the usual
 * case), so those dispatches are tracked as <em>abandoned</em> and reported via {@link
 * OutboxListener#onHandlerAbandoned} once {@code abandonedHandlerGrace} has passed. A dispatch of a
 * type that opted out is tracked and reported the same way, carrying {@code interrupted=false} —
 * its thread holds a pool slot just as long, it was simply never asked to stop, which is why it is
 * logged as a warning rather than an error.
 */
public final class WatchdogTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(WatchdogTask.class);

    private final InFlightRegistry inFlight;
    private final EventStore store;
    private final Clock clock;
    private final EventTypeConfigProvider typeConfig;
    private final OutboxListener listener;
    private final Duration abandonedGrace;

    public WatchdogTask(
            InFlightRegistry inFlight,
            EventStore store,
            Clock clock,
            EventTypeConfigProvider typeConfig,
            OutboxListener listener,
            Duration abandonedGrace) {
        this.inFlight = Objects.requireNonNull(inFlight);
        this.store = Objects.requireNonNull(store);
        this.clock = Objects.requireNonNull(clock);
        this.typeConfig = Objects.requireNonNull(typeConfig);
        this.listener = Objects.requireNonNull(listener);
        this.abandonedGrace = Objects.requireNonNull(abandonedGrace);
    }

    @Override
    public void run() {
        Instant now = clock.now();
        reclaimStuck(now);
        reportAbandoned(now);
    }

    private void reclaimStuck(Instant now) {
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
                if (!reclaimed) {
                    continue;
                }
                // Interrupt before bookkeeping so the recorded flag reflects what actually
                // happened: interruptIfActive() returns false when the dispatch finished in
                // between, and then there is no leaked thread to track either.
                boolean interrupted =
                        cfg.interruptStuckHandler() && entry.handle().interruptIfActive();
                inFlight.markAbandoned(entry, now, interrupted);
                listener.onStuckHandlerReclaimed(
                        new StuckHandlerReclaimedInfo(
                                entry.eventId(),
                                entry.eventType(),
                                elapsed,
                                entry.workerId(),
                                interrupted));
                log.warn(
                        "watchdog reclaimed stuck handler: eventId={} type={} elapsed={}"
                                + " thread={} interrupted={}",
                        entry.eventId(),
                        entry.eventType(),
                        elapsed,
                        entry.handle().threadName(),
                        interrupted);
            } catch (RuntimeException ex) {
                log.warn(
                        "watchdog force-reclaim failed for eventId={}: {}",
                        entry.eventId(),
                        ex.toString());
            }
        }
    }

    /**
     * Report force-reclaimed dispatches whose thread is still running well past the reclaim. One
     * alert per dispatch: a thread that never returns would otherwise log on every tick.
     *
     * <p>Guarded per entry for the same reason as {@link #reclaimStuck}: the watchdog runs under
     * {@code scheduleWithFixedDelay}, where a single escaping exception cancels every future tick.
     */
    private void reportAbandoned(Instant now) {
        for (Abandoned abandoned : inFlight.abandonedSnapshot()) {
            Entry entry = abandoned.entry();
            try {
                if (!entry.handle().isActive()) {
                    // Finished between the dispatch's deactivate() and its unregister(): no thread
                    // is held. Nothing else would evict it, so drop it here and let the gauge heal.
                    inFlight.unregister(entry);
                    continue;
                }
                if (Duration.between(abandoned.reclaimedAt(), now).compareTo(abandonedGrace) <= 0) {
                    continue;
                }
                if (!abandoned.claimReport()) {
                    continue;
                }
                Duration elapsed = Duration.between(entry.startedAt(), now);
                listener.onHandlerAbandoned(
                        new HandlerAbandonedInfo(
                                entry.eventId(),
                                entry.eventType(),
                                entry.workerId(),
                                entry.handle().threadName(),
                                elapsed,
                                abandoned.interrupted()));
                if (abandoned.interrupted()) {
                    log.error(
                            "handler ignored the interrupt and did not yield {} after"
                                    + " force-reclaim: eventId={} type={} thread={} running={};"
                                    + " this thread is lost to the {} pool until it returns —"
                                    + " configure a timeout on whatever it is blocked on",
                            abandonedGrace,
                            entry.eventId(),
                            entry.eventType(),
                            entry.handle().threadName(),
                            elapsed,
                            entry.eventType());
                } else {
                    // interruptStuckHandler is off for this type: the thread was never asked to
                    // stop, so this is the configured trade-off rather than a runaway handler.
                    log.warn(
                            "handler still running {} after force-reclaim: eventId={} type={}"
                                    + " thread={} running={}; interrupt-stuck-handler is off for"
                                    + " this type, so it keeps its slot of the {} pool until it"
                                    + " returns on its own",
                            abandonedGrace,
                            entry.eventId(),
                            entry.eventType(),
                            entry.handle().threadName(),
                            elapsed,
                            entry.eventType());
                }
            } catch (RuntimeException ex) {
                log.warn(
                        "watchdog failed to report abandoned dispatch for eventId={}: {}",
                        entry.eventId(),
                        ex.toString());
            }
        }
    }
}
