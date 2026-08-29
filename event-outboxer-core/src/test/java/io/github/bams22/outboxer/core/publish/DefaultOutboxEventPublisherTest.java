/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.publish;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bams22.outboxer.api.observer.EventPublishedInfo;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.publish.PublishOptions;
import io.github.bams22.outboxer.api.publish.PublishRequest;
import io.github.bams22.outboxer.core.support.StringEventSerializer;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.EventType;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.exception.NoTransactionException;
import io.github.bams22.outboxer.domain.exception.PublishValidationException;
import io.github.bams22.outboxer.spi.EventSerializer;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DefaultOutboxEventPublisherTest {

    @Test
    void publishPersistsEventAndFiresListener() {
        InMemoryEventStore store = new InMemoryEventStore();
        AtomicReference<EventPublishedInfo> captured = new AtomicReference<>();
        DefaultOutboxEventPublisher publisher =
                DefaultOutboxEventPublisher.builder()
                        .store(store)
                        .serializer(new StringEventSerializer())
                        .listener(
                                new OutboxListener() {
                                    @Override
                                    public void onEventPublished(EventPublishedInfo info) {
                                        captured.set(info);
                                    }
                                })
                        .build();

        UUID id = publisher.publish(EventType.of("T", String.class), "hello");

        Optional<Event> saved = store.findById(id);
        assertThat(saved).isPresent();
        assertThat(saved.orElseThrow().payload()).isEqualTo(SerializedPayload.ofText("hello"));
        assertThat(saved.orElseThrow().payloadFormat()).isEqualTo(StringEventSerializer.FORMAT);
        assertThat(saved.orElseThrow().status()).isEqualTo(EventStatus.PENDING);
        assertThat(captured.get()).isNotNull();
        assertThat(captured.get().eventId()).isEqualTo(id);
    }

    @Test
    void publishRespectsExplicitRunAt() {
        InMemoryEventStore store = new InMemoryEventStore();
        DefaultOutboxEventPublisher publisher = plain(store);

        Instant future = Instant.now().plusSeconds(3600);
        UUID id = publisher.publish(EventType.of("T", String.class), "payload", future);

        assertThat(store.findById(id).orElseThrow().runAt()).isEqualTo(future);
    }

    @Test
    void failsWhenNoTransactionActiveUnderFailPolicy() {
        DefaultOutboxEventPublisher publisher =
                DefaultOutboxEventPublisher.builder()
                        .store(new InMemoryEventStore())
                        .serializer(new StringEventSerializer())
                        .transactionContext(TransactionContext.neverActive())
                        .build();

        assertThatThrownBy(() -> publisher.publish(EventType.of("T", String.class), "hello"))
                .isInstanceOf(NoTransactionException.class);
    }

    @Test
    void ignorePolicyAllowsPublishWithoutTransaction() {
        InMemoryEventStore store = new InMemoryEventStore();
        DefaultOutboxEventPublisher publisher =
                DefaultOutboxEventPublisher.builder()
                        .store(store)
                        .serializer(new StringEventSerializer())
                        .transactionContext(TransactionContext.neverActive())
                        .noTransactionPolicy(NoTransactionPolicy.IGNORE)
                        .build();

        UUID id = publisher.publish(EventType.of("T", String.class), "hello");
        assertThat(store.findById(id)).isPresent();
    }

    @Test
    void rejectsNullType() {
        DefaultOutboxEventPublisher publisher = plain(new InMemoryEventStore());
        assertThatThrownBy(() -> publisher.publish((EventType<String>) null, "hello"))
                .isInstanceOf(PublishValidationException.class)
                .hasMessageContaining("type must not be null");
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void rejectsPayloadThatIsNotAnInstanceOfTheKeysClass() {
        DefaultOutboxEventPublisher publisher = plain(new InMemoryEventStore());
        // Only reachable by defeating generics (raw types, unchecked casts) — the typed API
        // catches this at compile time; the runtime check is the safety net (ADR-0031).
        EventType<Object> wrong = (EventType) EventType.of("T", Integer.class);

        assertThatThrownBy(() -> publisher.publish(wrong, "hello"))
                .isInstanceOf(PublishValidationException.class)
                .hasMessageContaining("java.lang.Integer")
                .hasMessageContaining("java.lang.String");
    }

    @Test
    void untypedKeyAcceptsAnyPayload() {
        InMemoryEventStore store = new InMemoryEventStore();
        DefaultOutboxEventPublisher publisher = plain(store);

        UUID id =
                publisher.publish(
                        EventType.untyped("DYNAMIC"), "any payload the serializer accepts");

        assertThat(store.findById(id)).isPresent();
    }

    @Test
    void wakeFiresOnlyAfterCommitAndOncePerType() {
        InMemoryEventStore store = new InMemoryEventStore();
        // TransactionContext that buffers afterCommit actions until the test "commits".
        List<Runnable> pendingCommitHooks = new ArrayList<>();
        TransactionContext buffering =
                new TransactionContext() {
                    @Override
                    public boolean isActive() {
                        return true;
                    }

                    @Override
                    public void afterCommit(Runnable action) {
                        pendingCommitHooks.add(action);
                    }
                };
        List<String> wakes = new ArrayList<>();
        DefaultOutboxEventPublisher publisher =
                DefaultOutboxEventPublisher.builder()
                        .store(store)
                        .serializer(new StringEventSerializer())
                        .transactionContext(buffering)
                        .waker(wakes::add)
                        .build();

        publisher.publishAll(
                List.of(
                        new PublishRequest<>(EventType.of("A", String.class), "a1", null),
                        new PublishRequest<>(EventType.of("A", String.class), "a2", null),
                        new PublishRequest<>(EventType.of("B", String.class), "b1", null)));
        publisher.publish(EventType.of("C", String.class), "c1");

        // Nothing may be woken before the surrounding transaction commits.
        assertThat(wakes).isEmpty();

        pendingCommitHooks.forEach(Runnable::run);

        // Each distinct type exactly once per publish call.
        assertThat(wakes).containsExactly("A", "B", "C");
    }

    @Test
    void throwingWakerDoesNotBreakPublish() {
        InMemoryEventStore store = new InMemoryEventStore();
        DefaultOutboxEventPublisher publisher =
                DefaultOutboxEventPublisher.builder()
                        .store(store)
                        .serializer(new StringEventSerializer())
                        .waker(
                                type -> {
                                    throw new IllegalStateException("waker exploded");
                                })
                        .build();

        UUID id = publisher.publish(EventType.of("T", String.class), "hello");

        assertThat(store.findById(id)).isPresent();
    }

    @Test
    void dedupKeyCoalescesIntoExistingPendingEvent() {
        InMemoryEventStore store = new InMemoryEventStore();
        AtomicInteger published = new AtomicInteger();
        List<String> wakes = new ArrayList<>();
        DefaultOutboxEventPublisher publisher =
                DefaultOutboxEventPublisher.builder()
                        .store(store)
                        .serializer(new StringEventSerializer())
                        .listener(
                                new OutboxListener() {
                                    @Override
                                    public void onEventPublished(EventPublishedInfo info) {
                                        published.incrementAndGet();
                                    }
                                })
                        .waker(wakes::add)
                        .build();
        PublishOptions keyed = PublishOptions.builder().dedupKey("order-1").build();

        UUID first = publisher.publish(EventType.of("SYNC", String.class), "v1", keyed);
        UUID second = publisher.publish(EventType.of("SYNC", String.class), "v2", keyed);
        UUID third =
                publisher.publish(
                        EventType.of("SYNC", String.class),
                        "v3",
                        PublishOptions.builder().dedupKey("order-2").build());

        assertThat(second).isEqualTo(first); // coalesced into the existing pending event
        assertThat(third).isNotEqualTo(first);
        assertThat(published).hasValue(2); // listener only for real inserts
        assertThat(wakes).containsExactly("SYNC", "SYNC"); // wake only for real inserts
        assertThat(store.findById(first)).isPresent();
    }

    @Test
    void publishAllRoutesDedupRequestsIndividually() {
        InMemoryEventStore store = new InMemoryEventStore();
        DefaultOutboxEventPublisher publisher = plain(store);
        PublishOptions keyed = PublishOptions.builder().dedupKey("k").build();

        List<UUID> ids =
                publisher.publishAll(
                        List.of(
                                new PublishRequest<>(EventType.of("A", String.class), "a1", keyed),
                                new PublishRequest<>(EventType.of("A", String.class), "a2", keyed),
                                new PublishRequest<>(
                                        EventType.of("A", String.class), "plain", null)));

        assertThat(ids).hasSize(3);
        assertThat(ids.get(1)).isEqualTo(ids.get(0)); // second keyed request coalesced
        assertThat(ids.get(2)).isNotEqualTo(ids.get(0));
        assertThat(store.findById(ids.get(0))).isPresent();
        assertThat(store.findById(ids.get(2))).isPresent();
    }

    @Test
    void perTypeOverrideWritesWithItsOwnSerializerAndFormat() {
        InMemoryEventStore store = new InMemoryEventStore();
        DefaultOutboxEventPublisher publisher = withOverride(store, "B");

        UUID plain = publisher.publish(EventType.of("A", String.class), "hello");
        UUID overridden = publisher.publish(EventType.of("B", String.class), "hello");

        assertThat(store.findById(plain).orElseThrow().payloadFormat())
                .isEqualTo(StringEventSerializer.FORMAT);
        Event b = store.findById(overridden).orElseThrow();
        assertThat(b.payloadFormat()).isEqualTo("test-alt");
        assertThat(b.payload()).isEqualTo(SerializedPayload.ofText("alt:hello"));
    }

    @Test
    void publishAllAppliesPerTypeOverridePerRequest() {
        InMemoryEventStore store = new InMemoryEventStore();
        DefaultOutboxEventPublisher publisher = withOverride(store, "B");

        List<UUID> ids =
                publisher.publishAll(
                        List.of(
                                new PublishRequest<>(EventType.of("A", String.class), "one", null),
                                new PublishRequest<>(
                                        EventType.of("B", String.class), "two", null)));

        assertThat(store.findById(ids.get(0)).orElseThrow().payloadFormat())
                .isEqualTo(StringEventSerializer.FORMAT);
        assertThat(store.findById(ids.get(1)).orElseThrow().payloadFormat()).isEqualTo("test-alt");
    }

    private static final OutboxListener NOOP = new OutboxListener() {};

    /** Distinct-format stub so per-type routing is observable in the stored payloadFormat. */
    private static final EventSerializer ALT_SERIALIZER =
            new EventSerializer() {
                @Override
                public String format() {
                    return "test-alt";
                }

                @Override
                public SerializedPayload serialize(Object payload) {
                    return SerializedPayload.ofText("alt:" + payload);
                }

                @Override
                public <T> T deserialize(SerializedPayload payload, Class<T> type) {
                    return type.cast(payload.requireText().substring("alt:".length()));
                }
            };

    /**
     * The builder's default path: only the two required collaborators, everything else substituted
     * in the constructor.
     */
    @Test
    void builderRequiresStoreAndSerializerAndDefaultsTheRest() {
        assertThatThrownBy(() -> DefaultOutboxEventPublisher.builder().build())
                .isInstanceOf(NullPointerException.class)
                .hasMessage("store must not be null");
        assertThatThrownBy(
                        () ->
                                DefaultOutboxEventPublisher.builder()
                                        .store(new InMemoryEventStore())
                                        .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessage("serializer must not be null");

        InMemoryEventStore store = new InMemoryEventStore();
        DefaultOutboxEventPublisher minimal =
                DefaultOutboxEventPublisher.builder()
                        .store(store)
                        .serializer(new StringEventSerializer())
                        .build();

        UUID id = minimal.publish(EventType.of("T", String.class), "hello");
        assertThat(store.findById(id)).isPresent();
    }

    private static DefaultOutboxEventPublisher withOverride(
            InMemoryEventStore store, String overriddenType) {
        return DefaultOutboxEventPublisher.builder()
                .store(store)
                .serializer(new StringEventSerializer())
                .writeSerializerOverrides(Map.of(overriddenType, ALT_SERIALIZER))
                .build();
    }

    private static DefaultOutboxEventPublisher plain(InMemoryEventStore store) {
        return DefaultOutboxEventPublisher.builder()
                .store(store)
                .serializer(new StringEventSerializer())
                .build();
    }
}
