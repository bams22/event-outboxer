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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.bams22.outboxer.api.observer.HandlerAbandonedInfo;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.observer.StuckHandlerReclaimedInfo;
import io.github.bams22.outboxer.core.config.EventTypeConfig;
import io.github.bams22.outboxer.core.config.EventTypeConfigProvider;
import io.github.bams22.outboxer.core.dispatch.DispatchHandle;
import io.github.bams22.outboxer.core.dispatch.InFlightRegistry;
import io.github.bams22.outboxer.core.dispatch.InFlightRegistry.Entry;
import io.github.bams22.outboxer.core.support.ForwardingEventStore;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Watchdog behaviour around a handler that outruns {@code handlerMaxRuntime}: the row is
 * force-reclaimed, the thread is asked to stop, and a thread that refuses to stop is reported once
 * as abandoned (ADR-0014).
 */
class WatchdogTaskTest {

    private static final String TYPE = "STUCK";
    private static final WorkerId WORKER = new WorkerId("w-1");

    private final InFlightRegistry inFlight = new InFlightRegistry();
    private final AtomicReference<Instant> now =
            new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));
    private final Clock clock = now::get;
    private final Recorder listener = new Recorder();
    private final AtomicInteger reclaims = new AtomicInteger();
    private final EventStore store =
            new ForwardingEventStore(new InMemoryEventStore()) {
                @Override
                public boolean forceReclaim(
                        UUID id, WorkerId workerId, long claimedVersion, Instant runAt) {
                    reclaims.incrementAndGet();
                    return true;
                }
            };
    private final List<Dispatch> dispatches = new ArrayList<>();

    @AfterEach
    void releaseThreads() {
        for (Dispatch d : dispatches) {
            d.finish();
        }
    }

    @Test
    @DisplayName("over-budget dispatch → row force-reclaimed, thread interrupted, entry abandoned")
    void interruptsStuckDispatch() {
        Dispatch d = dispatch("honours-interrupt", true);
        d.registerStartedAgo(Duration.ofMinutes(10));

        watchdog(true, Duration.ofSeconds(30)).run();

        assertThat(reclaims).hasValue(1);
        assertThat(d.awaitExit(Duration.ofSeconds(5))).isTrue();
        assertThat(d.interrupted).isTrue();
        assertThat(inFlight.size()).isZero();
        assertThat(inFlight.abandonedCount()).isEqualTo(1);
        assertThat(inFlight.abandonedCountByType(TYPE)).isEqualTo(1);
        assertThat(listener.stuck)
                .singleElement()
                .satisfies(
                        info -> {
                            assertThat(info.eventType()).isEqualTo(TYPE);
                            assertThat(info.elapsed()).isEqualTo(Duration.ofMinutes(10));
                            assertThat(info.interrupted()).isTrue();
                        });

        // The dispatch unwinds and deregisters itself: no thread is leaked after all.
        d.finish();
        assertThat(inFlight.abandonedCount()).isZero();
    }

    @Test
    @DisplayName("interruptStuckHandler=false → row still reclaimed, thread left running")
    void honoursInterruptOptOut() {
        Dispatch d = dispatch("not-interrupted", true);
        d.registerStartedAgo(Duration.ofMinutes(10));
        WatchdogTask watchdog = watchdog(false, Duration.ofSeconds(30));

        watchdog.run();

        assertThat(reclaims).hasValue(1);
        assertThat(d.awaitExit(Duration.ofMillis(200))).isFalse();
        assertThat(d.interrupted).isFalse();
        assertThat(inFlight.abandonedCount()).isEqualTo(1);
        assertThat(listener.stuck)
                .singleElement()
                .satisfies(i -> assertThat(i.interrupted()).isFalse());

        // The slot is held just as long, so it is still reported — flagged as "never asked to
        // stop", which is what separates the configured trade-off from a runaway handler.
        now.set(now.get().plusSeconds(31));
        watchdog.run();

        assertThat(listener.abandoned)
                .singleElement()
                .satisfies(i -> assertThat(i.interrupted()).isFalse());
    }

    @Test
    @DisplayName("thread that ignores the interrupt → reported abandoned exactly once")
    void reportsAbandonedOnceAfterGrace() {
        Dispatch d = dispatch("ignores-interrupt", false);
        d.registerStartedAgo(Duration.ofMinutes(10));
        WatchdogTask watchdog = watchdog(true, Duration.ofSeconds(30));

        watchdog.run();
        assertThat(listener.abandoned).isEmpty(); // grace has not elapsed yet

        now.set(now.get().plusSeconds(31));
        watchdog.run();
        watchdog.run();

        assertThat(listener.abandoned)
                .singleElement()
                .satisfies(
                        info -> {
                            assertThat(info.eventType()).isEqualTo(TYPE);
                            assertThat(info.threadName()).isEqualTo("ignores-interrupt");
                            assertThat(info.interrupted()).isTrue();
                            assertThat(info.elapsed())
                                    .isEqualTo(Duration.ofMinutes(10).plusSeconds(31));
                        });
        // Still counted as long as the thread holds its pool slot.
        assertThat(inFlight.abandonedCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("redelivery of the same event to this JVM keeps the zombie dispatch on the books")
    void redeliveryDoesNotEraseTheAbandonedDispatch() {
        Dispatch zombie = dispatch("zombie", false);
        zombie.registerStartedAgo(Duration.ofMinutes(10));
        WatchdogTask watchdog = watchdog(true, Duration.ofSeconds(30));

        watchdog.run();
        assertThat(inFlight.abandonedCountByType(TYPE)).isEqualTo(1);

        // The force-reclaimed row went back to PENDING and this JVM claimed it again: same event
        // id, another pool thread, and it completes normally while the zombie keeps running.
        Dispatch redelivered = dispatch(zombie.id, "redelivered", true);
        redelivered.registerStartedAgo(Duration.ZERO);
        redelivered.finish();

        assertThat(inFlight.size()).isZero();
        assertThat(inFlight.abandonedCountByType(TYPE)).isEqualTo(1);

        now.set(now.get().plusSeconds(31));
        watchdog.run();

        assertThat(listener.abandoned)
                .singleElement()
                .satisfies(info -> assertThat(info.threadName()).isEqualTo("zombie"));
    }

    @Test
    @DisplayName("an abandoned record whose dispatch already finished is evicted, not reported")
    void evictsAbandonedRecordOfFinishedDispatch() {
        Dispatch d = dispatch("finished-anyway", true);
        d.registerStartedAgo(Duration.ofMinutes(10));
        WatchdogTask watchdog = watchdog(true, Duration.ofSeconds(30));

        watchdog.run();
        assertThat(inFlight.abandonedCount()).isEqualTo(1);

        // Models the dispatch losing the race with markAbandoned: it detached and ran its own
        // unregister before the record existed, so nothing else will ever clean it up.
        d.deactivateWithoutUnregister();
        now.set(now.get().plusSeconds(31));
        watchdog.run();

        assertThat(inFlight.abandonedCount()).isZero();
        assertThat(listener.abandoned).isEmpty();
    }

    @Test
    @DisplayName("a throwing listener does not cancel the watchdog's next tick")
    void survivesThrowingListener() {
        Dispatch d = dispatch("ignores-interrupt", false);
        d.registerStartedAgo(Duration.ofMinutes(10));
        WatchdogTask watchdog =
                watchdog(
                        true,
                        Duration.ofSeconds(30),
                        new OutboxListener() {
                            @Override
                            public void onHandlerAbandoned(HandlerAbandonedInfo info) {
                                throw new IllegalStateException("listener blew up");
                            }
                        });

        watchdog.run();
        now.set(now.get().plusSeconds(31));

        assertThatCode(watchdog::run).doesNotThrowAnyException();
        assertThatCode(watchdog::run).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("dispatch within its budget is left alone")
    void leavesHealthyDispatchAlone() {
        Dispatch d = dispatch("healthy", true);
        d.registerStartedAgo(Duration.ofMinutes(1));

        watchdog(true, Duration.ofSeconds(30)).run();

        assertThat(reclaims).hasValue(0);
        assertThat(d.interrupted).isFalse();
        assertThat(inFlight.size()).isEqualTo(1);
        assertThat(inFlight.abandonedCount()).isZero();
        assertThat(listener.stuck).isEmpty();
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    private WatchdogTask watchdog(boolean interruptStuckHandler, Duration abandonedGrace) {
        return watchdog(interruptStuckHandler, abandonedGrace, listener);
    }

    private WatchdogTask watchdog(
            boolean interruptStuckHandler, Duration abandonedGrace, OutboxListener listener) {
        EventTypeConfig cfg =
                EventTypeConfig.defaults().toBuilder()
                        .handlerMaxRuntime(Duration.ofMinutes(5))
                        .interruptStuckHandler(interruptStuckHandler)
                        .build();
        return WatchdogTask.builder()
                .inFlight(inFlight)
                .store(store)
                .clock(clock)
                .typeConfig(new EventTypeConfigProvider(cfg, Map.of()))
                .listener(listener)
                .abandonedGrace(abandonedGrace)
                .build();
    }

    private Dispatch dispatch(String threadName, boolean honoursInterrupt) {
        return dispatch(UUID.randomUUID(), threadName, honoursInterrupt);
    }

    private Dispatch dispatch(UUID id, String threadName, boolean honoursInterrupt) {
        Dispatch d = new Dispatch(id, threadName, honoursInterrupt);
        dispatches.add(d);
        return d;
    }

    /** A running dispatch: a real thread parked in a handler, plus its registry bookkeeping. */
    private final class Dispatch {

        private final UUID id;
        private final CountDownLatch release = new CountDownLatch(1);
        private final CountDownLatch exited = new CountDownLatch(1);
        private final AtomicBoolean interrupted = new AtomicBoolean();
        private final Thread thread;
        private final DispatchHandle handle;
        private @Nullable Entry entry;

        private Dispatch(UUID id, String threadName, boolean honoursInterrupt) {
            this.id = id;
            CountDownLatch started = new CountDownLatch(1);
            this.thread =
                    new Thread(
                            () -> {
                                started.countDown();
                                if (honoursInterrupt) {
                                    try {
                                        release.await();
                                    } catch (InterruptedException e) {
                                        interrupted.set(true);
                                    }
                                } else {
                                    // Blocking I/O with no timeout, modelled: the interrupt is
                                    // noticed and thrown away.
                                    while (release.getCount() > 0) {
                                        try {
                                            release.await(50, TimeUnit.MILLISECONDS);
                                        } catch (InterruptedException e) {
                                            interrupted.set(true);
                                        }
                                    }
                                }
                                exited.countDown();
                            },
                            threadName);
            thread.start();
            awaitQuietly(started);
            this.handle = new DispatchHandle(thread);
        }

        private void registerStartedAgo(Duration age) {
            entry = new Entry(id, TYPE, WORKER, 7L, now.get().minus(age), handle);
            inFlight.register(entry);
        }

        private boolean awaitExit(Duration timeout) {
            try {
                return exited.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        /** What {@code HandlerDispatcher}'s finally block does once the handler returns. */
        private void finish() {
            release.countDown();
            awaitQuietly(exited);
            handle.deactivate();
            if (entry != null) {
                inFlight.unregister(entry);
            }
        }

        /** The half of {@link #finish()} that races {@code markAbandoned}. */
        private void deactivateWithoutUnregister() {
            release.countDown();
            awaitQuietly(exited);
            handle.deactivate();
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("latch did not open within 5s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static final class Recorder implements OutboxListener {

        private final List<StuckHandlerReclaimedInfo> stuck = new CopyOnWriteArrayList<>();
        private final List<HandlerAbandonedInfo> abandoned = new CopyOnWriteArrayList<>();

        @Override
        public void onStuckHandlerReclaimed(StuckHandlerReclaimedInfo info) {
            stuck.add(info);
        }

        @Override
        public void onHandlerAbandoned(HandlerAbandonedInfo info) {
            abandoned.add(info);
        }
    }
}
