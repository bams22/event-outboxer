/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.track;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.github.bams22.outboxer.api.track.AwaitResult;
import io.github.bams22.outboxer.api.track.TrackedState;
import io.github.bams22.outboxer.core.publish.TransactionContext;
import io.github.bams22.outboxer.domain.ArchivedEvent;
import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.domain.exception.AwaitInTransactionException;
import io.github.bams22.outboxer.domain.exception.AwaitInterruptedException;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.OutboxAdmin;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryOutboxAdmin;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DefaultOutboxEventTrackerTest {

    private static final String TYPE = "ORDER";
    private static final WorkerId WORKER = new WorkerId("tracker-test-worker");
    private static final Duration FAST = Duration.ofMillis(5);

    private final InMemoryEventStore store = new InMemoryEventStore();
    private final DefaultOutboxEventTracker tracker =
            DefaultOutboxEventTracker.builder()
                    .store(store)
                    .admin(new InMemoryOutboxAdmin(store))
                    .build();

    @Test
    void stateMapsTheHotTableStatus() {
        UUID id = save();
        assertThat(tracker.state(id)).isEqualTo(TrackedState.PENDING);

        ClaimedEvent claimed = claim();
        assertThat(tracker.state(id)).isEqualTo(TrackedState.PROCESSING);

        store.markDisabled(id, WORKER, claimed.claimedVersion(), "boom");
        assertThat(tracker.state(id)).isEqualTo(TrackedState.DISABLED);
    }

    @Test
    void stateWithoutAdminReportsAbsentForMissingRow() {
        DefaultOutboxEventTracker noAdmin =
                DefaultOutboxEventTracker.builder().store(store).build();
        assertThat(noAdmin.state(UUID.randomUUID())).isEqualTo(TrackedState.ABSENT);
    }

    @Test
    void stateConsultsTheArchiveForMissingRow() {
        UUID archivedId = UUID.randomUUID();
        OutboxAdmin admin = mock(OutboxAdmin.class);
        when(admin.findInArchive(archivedId)).thenReturn(Optional.of(archived(archivedId)));
        DefaultOutboxEventTracker withArchive =
                DefaultOutboxEventTracker.builder().store(store).admin(admin).build();

        assertThat(withArchive.state(archivedId)).isEqualTo(TrackedState.ARCHIVED);
        assertThat(withArchive.state(UUID.randomUUID())).isEqualTo(TrackedState.ABSENT);
    }

    @Test
    void awaitReturnsCompletedOnceTheRowIsFinalised() {
        UUID id = save();
        ClaimedEvent claimed = claim();
        CompletableFuture<Void> finaliser =
                CompletableFuture.runAsync(
                        () -> {
                            sleepQuietly(30);
                            store.markProcessed(id, WORKER, claimed.claimedVersion());
                        });

        long start = System.nanoTime();
        AwaitResult result = tracker.await(id, Duration.ofSeconds(2), FAST);

        assertThat(result).isEqualTo(new AwaitResult.Completed(id, null));
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(1));
        finaliser.join();
    }

    @Test
    void awaitCarriesTheArchiveRowWhenThereIsOne() {
        UUID id = UUID.randomUUID();
        ArchivedEvent row = archived(id);
        OutboxAdmin admin = mock(OutboxAdmin.class);
        when(admin.findInArchive(id)).thenReturn(Optional.of(row));
        DefaultOutboxEventTracker withArchive =
                DefaultOutboxEventTracker.builder().store(store).admin(admin).build();

        assertThat(withArchive.await(id, Duration.ofSeconds(1)))
                .isEqualTo(new AwaitResult.Completed(id, row));
    }

    @Test
    void awaitReturnsDisabledWithReasonAndAttempts() {
        UUID id = save();
        ClaimedEvent claimed = claim();
        store.markDisabled(id, WORKER, claimed.claimedVersion(), "permanent 404");

        AwaitResult result = tracker.await(id, Duration.ofSeconds(2), FAST);

        assertThat(result)
                .isInstanceOfSatisfying(
                        AwaitResult.Disabled.class,
                        d -> {
                            assertThat(d.eventId()).isEqualTo(id);
                            assertThat(d.event().lastFailReason()).isEqualTo("permanent 404");
                            assertThat(d.event().attempts())
                                    .isEqualTo(store.findById(id).orElseThrow().attempts());
                        });
    }

    @Test
    void awaitTimesOutOnAnUntouchedRow() {
        UUID id = save();

        AwaitResult result = tracker.await(id, Duration.ofMillis(50), FAST);

        assertThat(result)
                .isInstanceOfSatisfying(
                        AwaitResult.TimedOut.class,
                        t -> {
                            assertThat(t.eventId()).isEqualTo(id);
                            assertThat(t.lastSeen()).isEqualTo(TrackedState.PENDING);
                            assertThat(t.waited()).isGreaterThanOrEqualTo(Duration.ofMillis(50));
                        });
    }

    @Test
    void awaitInsideATransactionFailsFastWithoutQuerying() {
        EventStore untouched = mock(EventStore.class);
        DefaultOutboxEventTracker guarded =
                DefaultOutboxEventTracker.builder()
                        .store(untouched)
                        .transactionContext(TransactionContext.alwaysActive())
                        .build();

        assertThatThrownBy(() -> guarded.await(UUID.randomUUID(), Duration.ofSeconds(10)))
                .isInstanceOf(AwaitInTransactionException.class)
                .hasMessageStartingWith("OUTBOX-501:")
                .hasMessageContaining("after the publishing transaction commits");
        verifyNoInteractions(untouched);
    }

    @Test
    void interruptRestoresTheFlagAndThrows() throws Exception {
        UUID id = save();
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        AtomicReference<Boolean> interruptedAfter = new AtomicReference<>();
        Thread waiter =
                Thread.ofPlatform()
                        .start(
                                () -> {
                                    try {
                                        tracker.await(id, Duration.ofSeconds(10), FAST);
                                    } catch (Throwable t) {
                                        thrown.set(t);
                                    }
                                    interruptedAfter.set(Thread.currentThread().isInterrupted());
                                });
        sleepQuietly(30);
        waiter.interrupt();
        assertThat(waiter.join(Duration.ofSeconds(2))).isTrue();

        assertThat(thrown.get())
                .isInstanceOf(AwaitInterruptedException.class)
                .hasMessageStartingWith("OUTBOX-502:")
                .hasCauseInstanceOf(InterruptedException.class);
        assertThat(interruptedAfter.get()).isTrue();
    }

    @Test
    void hugeTimeoutSaturatesInsteadOfOverflowing() {
        UUID id = UUID.randomUUID();

        // Duration.toNanos() overflows past ~292 years; the wait must still run normally.
        assertThat(tracker.await(id, Duration.ofSeconds(Long.MAX_VALUE)))
                .isEqualTo(new AwaitResult.Completed(id, null));
    }

    @Test
    void rejectsInvalidArguments() {
        UUID id = UUID.randomUUID();
        assertThatThrownBy(() -> tracker.state(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> tracker.await(null, Duration.ofSeconds(1)))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> tracker.await(id, null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> tracker.await(id, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("timeout");
        assertThatThrownBy(() -> tracker.await(id, Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> tracker.await(id, Duration.ofSeconds(1), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pollInterval");
        assertThatThrownBy(() -> tracker.await(id, Duration.ofSeconds(1), Duration.ofMillis(-5)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void builderValidatesItsCollaborators() {
        assertThatThrownBy(() -> DefaultOutboxEventTracker.builder().build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("store must not be null");
        assertThatThrownBy(
                        () ->
                                DefaultOutboxEventTracker.builder()
                                        .store(store)
                                        .pollInterval(Duration.ZERO)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pollInterval");
    }

    private UUID save() {
        UUID id = UUID.randomUUID();
        store.save(
                PendingEvent.builder()
                        .id(id)
                        .eventType(TYPE)
                        .payload(SerializedPayload.ofText("p"))
                        .payloadFormat("text")
                        .payloadClass("java.lang.String")
                        .priority((short) 0)
                        .runAt(Instant.now().minusSeconds(1))
                        .traceContext(Map.of())
                        .build());
        return id;
    }

    private ClaimedEvent claim() {
        return store.claim(new ClaimRequest(TYPE, WORKER, 1)).get(0);
    }

    private static ArchivedEvent archived(UUID id) {
        Instant now = Instant.now();
        return ArchivedEvent.builder()
                .id(id)
                .eventType(TYPE)
                .payload(SerializedPayload.ofText("p"))
                .payloadFormat("text")
                .payloadClass("java.lang.String")
                .attempts(1)
                .createdAt(now)
                .runAt(now)
                .traceContext(Map.of())
                .archivedAt(now)
                .archivedBy(WORKER.value())
                .build();
    }

    private static void sleepQuietly(long millis) {
        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
