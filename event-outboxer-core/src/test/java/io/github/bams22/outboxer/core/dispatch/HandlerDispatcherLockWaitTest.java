/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.dispatch;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.api.observer.EventProcessedInfo;
import io.github.bams22.outboxer.api.observer.EventRetryScheduledInfo;
import io.github.bams22.outboxer.api.observer.LockAcquiredInfo;
import io.github.bams22.outboxer.api.observer.LockAcquisitionInfo;
import io.github.bams22.outboxer.api.observer.LockReleasedInfo;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.core.config.EventTypeConfig;
import io.github.bams22.outboxer.core.config.EventTypeConfigProvider;
import io.github.bams22.outboxer.core.support.ForwardingEventStore;
import io.github.bams22.outboxer.core.support.StringEventSerializer;
import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.domain.EventType;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.domain.exception.LockAcquisitionException;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.spi.EntityLocker.LockHandle;
import io.github.bams22.outboxer.spi.EventSerializerRegistry;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEntityLocker;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The bounded lock wait of ADR-0035 as the dispatcher applies it: {@code lockWait = 0} keeps the
 * one-attempt-then-release flow of ADR-0012 byte for byte; a non-zero wait keeps the claimed event
 * on the handler thread until the key frees or the budget is spent; and an interrupt that cuts the
 * wait short never lets a storage call run on an interrupted thread.
 */
class HandlerDispatcherLockWaitTest {

    private static final WorkerId WORKER = new WorkerId("lock-wait-test-worker");
    private static final String TYPE = "LOCKED";
    // StringEventSerializer keeps the JSON quotes: payload "\"p\"" -> key lock:"p".
    private static final String KEY = "lock:\"p\"";

    private final InMemoryEventStore delegate = new InMemoryEventStore();
    private final RecordingListener listener = new RecordingListener();
    private final AtomicInteger releaseCalls = new AtomicInteger();
    private final AtomicBoolean interruptedDuringRelease = new AtomicBoolean();
    private final CopyOnWriteArrayList<String> releaseReasons = new CopyOnWriteArrayList<>();
    private final AtomicInteger handled = new AtomicInteger();

    private final EventStore store =
            new ForwardingEventStore(delegate) {
                @Override
                public boolean release(
                        UUID id,
                        WorkerId workerId,
                        long claimedVersion,
                        String reason,
                        Instant runAt) {
                    releaseCalls.incrementAndGet();
                    releaseReasons.add(reason);
                    interruptedDuringRelease.set(Thread.currentThread().isInterrupted());
                    return delegate.release(id, workerId, claimedVersion, reason, runAt);
                }
            };

    @Test
    @DisplayName("lockWait=0: one non-blocking attempt, then the ADR-0012 release path unchanged")
    void zeroWait_singleAttemptThenRelease() {
        AtomicInteger plainCalls = new AtomicInteger();
        AtomicInteger waitCalls = new AtomicInteger();
        EntityLocker alwaysBusy =
                new EntityLocker() {
                    @Override
                    public Optional<LockHandle> tryLock(String key, Duration ttl) {
                        plainCalls.incrementAndGet();
                        return Optional.empty();
                    }

                    @Override
                    public Optional<LockHandle> tryLock(
                            String key, Duration ttl, Duration maxWait) {
                        waitCalls.incrementAndGet();
                        return Optional.empty();
                    }
                };
        Instant now = Instant.parse("2026-09-04T10:00:00Z");
        HandlerDispatcher dispatcher =
                dispatcher(alwaysBusy, Duration.ZERO, () -> now, new InFlightRegistry());
        ClaimedEvent claimed = saveAndClaim();

        dispatcher.dispatch(claimed);

        assertThat(plainCalls).hasValue(1);
        assertThat(waitCalls).as("zero wait must not touch the waiting overload").hasValue(0);
        assertThat(handled).hasValue(0);
        assertThat(releaseCalls).hasValue(1);
        assertThat(releaseReasons).containsExactly("lock busy: " + KEY);
        assertThat(listener.acquired).isEmpty();
        assertThat(listener.failed).hasSize(1);
        LockAcquisitionInfo failed = listener.failed.get(0);
        assertThat(failed.outcome()).isEqualTo(LockAcquisitionInfo.Outcome.BUSY);
        assertThat(failed.lockKey()).isEqualTo(KEY);
        assertThat(failed.waited()).isEqualTo(Duration.ZERO);
        assertThat(listener.retries).hasSize(1);
        EventRetryScheduledInfo retry = listener.retries.get(0);
        assertThat(retry.trigger()).isEqualTo(EventRetryScheduledInfo.Trigger.LOCK_BUSY);
        // Contention does not consume the retry budget: attempts is still the first attempt.
        assertThat(retry.attempts()).isEqualTo(1);
        assertThat(retry.nextRunAt())
                .isEqualTo(now.plus(DispatcherConfig.defaults().lockBusyRetryDelay()));
    }

    @Test
    @DisplayName("the handler runs once the holder releases inside the wait window")
    void wait_holderReleasesInsideWindow_handlerRuns() throws Exception {
        InMemoryEntityLocker locker = new InMemoryEntityLocker();
        LockHandle holder = locker.tryLock(KEY, Duration.ofSeconds(30)).orElseThrow();
        Duration holdFor = Duration.ofMillis(60);
        Thread releaser =
                new Thread(
                        () -> {
                            sleepQuietly(holdFor);
                            holder.close();
                        },
                        "lock-releaser");
        HandlerDispatcher dispatcher =
                dispatcher(locker, Duration.ofSeconds(5), null, new InFlightRegistry());
        ClaimedEvent claimed = saveAndClaim();

        releaser.start();
        dispatcher.dispatch(claimed);
        releaser.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(handled).hasValue(1);
        assertThat(releaseCalls).as("no round trip to PENDING").hasValue(0);
        assertThat(listener.failed).isEmpty();
        assertThat(listener.retries).isEmpty();
        assertThat(listener.processed).hasSize(1);
        assertThat(listener.acquired).hasSize(1);
        LockAcquiredInfo acquired = listener.acquired.get(0);
        assertThat(acquired.lockKey()).isEqualTo(KEY);
        assertThat(acquired.waited()).isGreaterThanOrEqualTo(holdFor);
        // Hold time is reported on release, key included.
        assertThat(listener.released).hasSize(1);
        assertThat(listener.released.get(0).lockKey()).isEqualTo(KEY);
        assertThat(listener.released.get(0).held()).isGreaterThanOrEqualTo(Duration.ZERO);
        // The lock was released after the handler: the key is free again.
        assertThat(locker.tryLock(KEY, Duration.ofSeconds(1))).isPresent();
    }

    @Test
    @DisplayName("a holder that outlasts the window sends the event down the busy path after it")
    void wait_holderKeepsKey_releasesAfterBudget() {
        InMemoryEntityLocker locker = new InMemoryEntityLocker();
        Duration budget = Duration.ofMillis(120);
        try (LockHandle _ = locker.tryLock(KEY, Duration.ofSeconds(30)).orElseThrow()) {
            HandlerDispatcher dispatcher = dispatcher(locker, budget, null, new InFlightRegistry());
            ClaimedEvent claimed = saveAndClaim();
            long start = System.nanoTime();

            dispatcher.dispatch(claimed);

            Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
            assertThat(elapsed).isGreaterThanOrEqualTo(budget);
            assertThat(elapsed).isLessThan(budget.plusSeconds(5));
            assertThat(handled).hasValue(0);
            assertThat(releaseCalls).hasValue(1);
            assertThat(listener.acquired).isEmpty();
            assertThat(listener.failed).hasSize(1);
            assertThat(listener.failed.get(0).outcome())
                    .isEqualTo(LockAcquisitionInfo.Outcome.BUSY);
            assertThat(listener.failed.get(0).waited()).isGreaterThanOrEqualTo(budget);
            assertThat(listener.retries).hasSize(1);
            assertThat(listener.retries.get(0).trigger())
                    .isEqualTo(EventRetryScheduledInfo.Trigger.LOCK_BUSY);
            assertThat(listener.retries.get(0).attempts()).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("a watchdog interrupt ends the wait at once and is consumed before the release")
    void wait_watchdogInterrupt_returnsPromptlyAndReleasesOnCleanThread() throws Exception {
        CountDownLatch firstAttempt = new CountDownLatch(1);
        EntityLocker alwaysBusy =
                (key, ttl) -> {
                    firstAttempt.countDown();
                    return Optional.empty();
                };
        InFlightRegistry inFlight = new InFlightRegistry();
        HandlerDispatcher dispatcher =
                dispatcher(alwaysBusy, Duration.ofSeconds(30), null, inFlight);
        ClaimedEvent claimed = saveAndClaim();
        AtomicBoolean interruptedAfterDispatch = new AtomicBoolean();
        Thread pool =
                new Thread(
                        () -> {
                            dispatcher.dispatch(claimed);
                            interruptedAfterDispatch.set(Thread.currentThread().isInterrupted());
                        },
                        "outbox-LOCKED-1");
        pool.start();
        assertThat(firstAttempt.await(5, TimeUnit.SECONDS)).isTrue();

        // What WatchdogTask does after a successful forceReclaim.
        InFlightRegistry.Entry entry = inFlight.snapshot().iterator().next();
        assertThat(entry.handle().interruptIfActive()).isTrue();
        pool.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(pool.isAlive()).as("the wait must not run out its 30 s budget").isFalse();
        assertThat(handled).hasValue(0);
        assertThat(listener.failed).hasSize(1);
        assertThat(listener.failed.get(0).waited()).isLessThan(Duration.ofSeconds(5));
        // The release still runs (a no-op after a real force-reclaim, ADR-0014) — on a clean
        // thread.
        assertThat(releaseCalls).hasValue(1);
        assertThat(interruptedDuringRelease).isFalse();
        assertThat(interruptedAfterDispatch).isFalse();
        assertThat(inFlight.size()).isZero();
    }

    @Test
    @DisplayName("an executor shutdown interrupt ends the wait, skips storage and keeps the flag")
    void wait_shutdownInterrupt_returnsPromptlyWithoutStorage() throws Exception {
        CountDownLatch firstAttempt = new CountDownLatch(1);
        EntityLocker alwaysBusy =
                (key, ttl) -> {
                    firstAttempt.countDown();
                    return Optional.empty();
                };
        InFlightRegistry inFlight = new InFlightRegistry();
        HandlerDispatcher dispatcher =
                dispatcher(alwaysBusy, Duration.ofSeconds(30), null, inFlight);
        ClaimedEvent claimed = saveAndClaim();
        AtomicBoolean interruptedAfterDispatch = new AtomicBoolean();
        Thread pool =
                new Thread(
                        () -> {
                            dispatcher.dispatch(claimed);
                            interruptedAfterDispatch.set(Thread.currentThread().isInterrupted());
                        },
                        "outbox-LOCKED-1");
        pool.start();
        assertThat(firstAttempt.await(5, TimeUnit.SECONDS)).isTrue();

        // What ExecutorService.shutdownNow() does to a running task — not routed via the handle.
        pool.interrupt();
        pool.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(pool.isAlive()).as("the wait must not run out its 30 s budget").isFalse();
        assertThat(handled).hasValue(0);
        assertThat(listener.failed).hasSize(1);
        assertThat(releaseCalls)
                .as("no storage call on a thread interrupted by shutdown; releaseClaimed covers it")
                .hasValue(0);
        assertThat(listener.retries).isEmpty();
        assertThat(interruptedAfterDispatch).as("a foreign interrupt is left standing").isTrue();
        assertThat(inFlight.size()).isZero();
    }

    @Test
    @DisplayName("a locker error during the wait takes the ERROR path unchanged")
    void wait_backendError_errorPathUnchanged() {
        EntityLocker broken =
                (key, ttl) -> {
                    throw new LockAcquisitionException("redis down", null);
                };
        HandlerDispatcher dispatcher =
                dispatcher(broken, Duration.ofMillis(200), null, new InFlightRegistry());
        ClaimedEvent claimed = saveAndClaim();

        dispatcher.dispatch(claimed);

        assertThat(handled).hasValue(0);
        assertThat(listener.failed).hasSize(1);
        assertThat(listener.failed.get(0).outcome()).isEqualTo(LockAcquisitionInfo.Outcome.ERROR);
        assertThat(listener.failed.get(0).cause()).isInstanceOf(LockAcquisitionException.class);
        assertThat(releaseCalls).hasValue(1);
        assertThat(releaseReasons.get(0)).startsWith("lock acquisition error:");
    }

    @Test
    @DisplayName("a handler without a lock key never touches the locker, whatever lockWait says")
    void wait_noLockKey_lockerUntouched() {
        EntityLocker mustNotBeCalled =
                (key, ttl) -> {
                    throw new AssertionError("locker must not be consulted without a lock key");
                };
        HandlerDispatcher dispatcher =
                dispatcher(
                        mustNotBeCalled,
                        Duration.ofMillis(200),
                        null,
                        new InFlightRegistry(),
                        false);
        ClaimedEvent claimed = saveAndClaim();

        dispatcher.dispatch(claimed);

        assertThat(handled).hasValue(1);
        assertThat(listener.acquired).isEmpty();
        assertThat(listener.failed).isEmpty();
        assertThat(listener.processed).hasSize(1);
    }

    private HandlerDispatcher dispatcher(
            EntityLocker locker,
            Duration lockWait,
            io.github.bams22.outboxer.spi.@Nullable Clock clock,
            InFlightRegistry inFlight) {
        return dispatcher(locker, lockWait, clock, inFlight, true);
    }

    private HandlerDispatcher dispatcher(
            EntityLocker locker,
            Duration lockWait,
            io.github.bams22.outboxer.spi.@Nullable Clock clock,
            InFlightRegistry inFlight,
            boolean withLockKey) {
        EventHandler<String> handler =
                new EventHandler<String>() {
                    @Override
                    public EventType<String> type() {
                        return EventType.of(TYPE, String.class);
                    }

                    @Override
                    public @Nullable String extractLockKey(String payload) {
                        return withLockKey ? "lock:" + payload : null;
                    }

                    @Override
                    public EventOutcome handle(EventContext ctx, String payload) {
                        handled.incrementAndGet();
                        return EventOutcome.success();
                    }
                };
        EventTypeConfig cfg =
                EventTypeConfig.defaults().toBuilder()
                        .handlerMaxRuntime(Duration.ofMinutes(1))
                        .lockTtl(Duration.ofMinutes(2))
                        .lockWait(lockWait)
                        .build();
        return HandlerDispatcher.builder()
                .store(store)
                .locker(locker)
                .serializerRegistry(
                        EventSerializerRegistry.of(List.of(new StringEventSerializer())))
                .handlerResolver(new EventHandlerResolver(List.of(handler)))
                .inFlight(inFlight)
                .listener(listener)
                .clock(clock)
                .typeConfig(EventTypeConfigProvider.uniform(cfg))
                .workerId(WORKER)
                .build();
    }

    private ClaimedEvent saveAndClaim() {
        delegate.save(
                PendingEvent.builder()
                        .id(UUID.randomUUID())
                        .eventType(TYPE)
                        .payload(SerializedPayload.ofText("\"p\""))
                        .payloadFormat(StringEventSerializer.FORMAT)
                        .payloadClass("java.lang.String")
                        .priority((short) 0)
                        .runAt(Instant.now().minusSeconds(1))
                        .traceContext(Map.of())
                        .build());
        return delegate.claim(new ClaimRequest(TYPE, WORKER, 10)).get(0);
    }

    private static void sleepQuietly(Duration d) {
        try {
            Thread.sleep(d);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class RecordingListener implements OutboxListener {
        final CopyOnWriteArrayList<LockAcquiredInfo> acquired = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<LockAcquisitionInfo> failed = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<EventRetryScheduledInfo> retries = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<EventProcessedInfo> processed = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<LockReleasedInfo> released = new CopyOnWriteArrayList<>();

        @Override
        public void onLockAcquired(LockAcquiredInfo info) {
            acquired.add(info);
        }

        @Override
        public void onLockReleased(LockReleasedInfo info) {
            released.add(info);
        }

        @Override
        public void onLockAcquisitionFailed(LockAcquisitionInfo info) {
            failed.add(info);
        }

        @Override
        public void onEventRetryScheduled(EventRetryScheduledInfo info) {
            retries.add(info);
        }

        @Override
        public void onEventProcessed(EventProcessedInfo info) {
            processed.add(info);
        }
    }
}
