/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.api.publish.OutboxEventPublisher;
import io.github.bams22.outboxer.api.track.AwaitResult;
import io.github.bams22.outboxer.api.track.OutboxEventTracker;
import io.github.bams22.outboxer.api.track.TrackedState;
import io.github.bams22.outboxer.domain.EventType;
import io.github.bams22.outboxer.domain.exception.AwaitInTransactionException;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.liquibase.LiquibaseAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.annotation.Transactional;

/**
 * End-to-end {@code OutboxEventTracker} over PostgreSQL (ADR-0038). Two concrete subclasses run the
 * same scenarios with the archive off ({@link PostgresTrackerIT}) and on ({@link
 * PostgresTrackerArchiveIT}); each brings its own container, and the context is closed after the
 * class so its engine does not keep polling a database the other class owns.
 */
@SpringBootTest(
        classes = PostgresTrackerITBase.TestApp.class,
        properties = {
            "event-outboxer.storage.type=postgres",
            "event-outboxer.publisher.no-transaction-policy=FAIL",
            "event-outboxer.event-types.defaults.poll-min-interval=20ms",
            "event-outboxer.event-types.defaults.poll-max-interval=50ms",
            "event-outboxer.tracker.poll-interval=20ms",
            "event-outboxer.maintenance.shutdown-timeout=2s"
        })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
abstract class PostgresTrackerITBase {

    static final EventType<Tracked> TYPE = EventType.of("TRACKED", Tracked.class);

    @Autowired OutboxEventTracker tracker;
    @Autowired TrackedService service;

    /**
     * {@code true} when the subclass runs with {@code event-outboxer.storage.archive-enabled=true}.
     */
    abstract boolean archiveEnabled();

    @Test
    void committedEventCompletes() {
        UUID id = service.publish(new Tracked("ok", false));

        AwaitResult result = tracker.await(id, Duration.ofSeconds(10));

        assertThat(result).isInstanceOf(AwaitResult.Completed.class);
        AwaitResult.Completed completed = (AwaitResult.Completed) result;
        assertThat(completed.eventId()).isEqualTo(id);
        if (archiveEnabled()) {
            assertThat(completed.archived()).isNotNull();
            assertThat(completed.archived().id()).isEqualTo(id);
            assertThat(tracker.state(id)).isEqualTo(TrackedState.ARCHIVED);
        } else {
            assertThat(completed.archived()).isNull();
            assertThat(tracker.state(id)).isEqualTo(TrackedState.ABSENT);
        }
    }

    @Test
    void failedEventIsReportedDisabled() {
        UUID id = service.publish(new Tracked("broken", true));

        AwaitResult result = tracker.await(id, Duration.ofSeconds(10));

        assertThat(result)
                .isInstanceOfSatisfying(
                        AwaitResult.Disabled.class,
                        d -> assertThat(d.event().lastFailReason()).startsWith("rejected: broken"));
        assertThat(tracker.state(id)).isEqualTo(TrackedState.DISABLED);
    }

    @Test
    void awaitInsideTheTransactionFailsFast() {
        long start = System.nanoTime();

        assertThatThrownBy(() -> service.publishAndAwait(new Tracked("too-early", false)))
                .isInstanceOf(AwaitInTransactionException.class);

        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(5));
    }

    @Test
    void unknownIdIsAbsent() {
        assertThat(tracker.state(UUID.randomUUID())).isEqualTo(TrackedState.ABSENT);
    }

    record Tracked(String ref, boolean fail) {}

    static class TrackedHandler implements EventHandler<Tracked> {

        @Override
        public EventType<Tracked> type() {
            return TYPE;
        }

        @Override
        public EventOutcome handle(EventContext ctx, Tracked payload) {
            return payload.fail()
                    ? EventOutcome.fail("rejected: " + payload.ref())
                    : EventOutcome.success();
        }
    }

    /**
     * Brackets the publishing transaction so the test can wait after commit — or, deliberately,
     * before it.
     */
    static class TrackedService {
        private final OutboxEventPublisher publisher;
        private final OutboxEventTracker tracker;

        TrackedService(OutboxEventPublisher publisher, OutboxEventTracker tracker) {
            this.publisher = publisher;
            this.tracker = tracker;
        }

        @Transactional
        UUID publish(Tracked payload) {
            return publisher.publish(TYPE, payload);
        }

        @Transactional
        AwaitResult publishAndAwait(Tracked payload) {
            UUID id = publisher.publish(TYPE, payload);
            return tracker.await(id, Duration.ofSeconds(30));
        }
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration(exclude = LiquibaseAutoConfiguration.class)
    static class TestApp {

        @Bean
        TrackedHandler trackedHandler() {
            return new TrackedHandler();
        }

        @Bean
        TrackedService trackedService(OutboxEventPublisher publisher, OutboxEventTracker tracker) {
            return new TrackedService(publisher, tracker);
        }
    }
}
