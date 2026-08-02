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
import io.github.bams22.outboxer.core.polling.PollerWaker;
import io.github.bams22.outboxer.core.support.StringEventSerializer;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.exception.NoTransactionException;
import io.github.bams22.outboxer.domain.exception.PublishValidationException;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DefaultOutboxEventPublisherTest {

  @Test
  void publishPersistsEventAndFiresListener() {
    InMemoryEventStore store = new InMemoryEventStore();
    AtomicReference<EventPublishedInfo> captured = new AtomicReference<>();
    DefaultOutboxEventPublisher publisher =
        new DefaultOutboxEventPublisher(
            store,
            new StringEventSerializer(),
            Clock.system(),
            TransactionContext.alwaysActive(),
            NoTransactionPolicy.FAIL,
            new OutboxListener() {
              @Override
              public void onEventPublished(EventPublishedInfo info) {
                captured.set(info);
              }
            },
            PollerWaker.NOOP);

    UUID id = publisher.publish("T", "hello");

    Optional<Event> saved = store.findById(id);
    assertThat(saved).isPresent();
    assertThat(saved.orElseThrow().payload())
        .isEqualTo(io.github.bams22.outboxer.domain.SerializedPayload.ofText("hello"));
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
    UUID id = publisher.publish("T", "payload", future);

    assertThat(store.findById(id).orElseThrow().runAt()).isEqualTo(future);
  }

  @Test
  void failsWhenNoTransactionActiveUnderFailPolicy() {
    DefaultOutboxEventPublisher publisher =
        new DefaultOutboxEventPublisher(
            new InMemoryEventStore(),
            new StringEventSerializer(),
            Clock.system(),
            TransactionContext.neverActive(),
            NoTransactionPolicy.FAIL,
            NOOP,
            PollerWaker.NOOP);

    assertThatThrownBy(() -> publisher.publish("T", "hello"))
        .isInstanceOf(NoTransactionException.class);
  }

  @Test
  void ignorePolicyAllowsPublishWithoutTransaction() {
    InMemoryEventStore store = new InMemoryEventStore();
    DefaultOutboxEventPublisher publisher =
        new DefaultOutboxEventPublisher(
            store,
            new StringEventSerializer(),
            Clock.system(),
            TransactionContext.neverActive(),
            NoTransactionPolicy.IGNORE,
            NOOP,
            PollerWaker.NOOP);

    UUID id = publisher.publish("T", "hello");
    assertThat(store.findById(id)).isPresent();
  }

  @Test
  void rejectsBlankEventType() {
    DefaultOutboxEventPublisher publisher = plain(new InMemoryEventStore());
    assertThatThrownBy(() -> publisher.publish("", "hello"))
        .isInstanceOf(PublishValidationException.class);
  }

  @Test
  void wakeFiresOnlyAfterCommitAndOncePerType() {
    InMemoryEventStore store = new InMemoryEventStore();
    // TransactionContext that buffers afterCommit actions until the test "commits".
    java.util.List<Runnable> pendingCommitHooks = new java.util.ArrayList<>();
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
    java.util.List<String> wakes = new java.util.ArrayList<>();
    DefaultOutboxEventPublisher publisher =
        new DefaultOutboxEventPublisher(
            store,
            new StringEventSerializer(),
            Clock.system(),
            buffering,
            NoTransactionPolicy.FAIL,
            NOOP,
            wakes::add);

    publisher.publishAll(
        java.util.List.of(
            new io.github.bams22.outboxer.api.publish.PublishRequest("A", "a1", null),
            new io.github.bams22.outboxer.api.publish.PublishRequest("A", "a2", null),
            new io.github.bams22.outboxer.api.publish.PublishRequest("B", "b1", null)));
    publisher.publish("C", "c1");

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
        new DefaultOutboxEventPublisher(
            store,
            new StringEventSerializer(),
            Clock.system(),
            TransactionContext.alwaysActive(),
            NoTransactionPolicy.FAIL,
            NOOP,
            type -> {
              throw new IllegalStateException("waker exploded");
            });

    UUID id = publisher.publish("T", "hello");

    assertThat(store.findById(id)).isPresent();
  }

  @Test
  void dedupKeyCoalescesIntoExistingPendingEvent() {
    InMemoryEventStore store = new InMemoryEventStore();
    java.util.concurrent.atomic.AtomicInteger published =
        new java.util.concurrent.atomic.AtomicInteger();
    java.util.List<String> wakes = new java.util.ArrayList<>();
    DefaultOutboxEventPublisher publisher =
        new DefaultOutboxEventPublisher(
            store,
            new StringEventSerializer(),
            Clock.system(),
            TransactionContext.alwaysActive(),
            NoTransactionPolicy.FAIL,
            new OutboxListener() {
              @Override
              public void onEventPublished(EventPublishedInfo info) {
                published.incrementAndGet();
              }
            },
            wakes::add);
    PublishOptions keyed = PublishOptions.builder().dedupKey("order-1").build();

    UUID first = publisher.publish("SYNC", "v1", keyed);
    UUID second = publisher.publish("SYNC", "v2", keyed);
    UUID third =
        publisher.publish("SYNC", "v3", PublishOptions.builder().dedupKey("order-2").build());

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

    java.util.List<UUID> ids =
        publisher.publishAll(
            java.util.List.of(
                new io.github.bams22.outboxer.api.publish.PublishRequest("A", "a1", keyed),
                new io.github.bams22.outboxer.api.publish.PublishRequest("A", "a2", keyed),
                new io.github.bams22.outboxer.api.publish.PublishRequest("A", "plain", null)));

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

    UUID plain = publisher.publish("A", "hello");
    UUID overridden = publisher.publish("B", "hello");

    assertThat(store.findById(plain).orElseThrow().payloadFormat())
        .isEqualTo(StringEventSerializer.FORMAT);
    Event b = store.findById(overridden).orElseThrow();
    assertThat(b.payloadFormat()).isEqualTo("test-alt");
    assertThat(b.payload())
        .isEqualTo(io.github.bams22.outboxer.domain.SerializedPayload.ofText("alt:hello"));
  }

  @Test
  void publishAllAppliesPerTypeOverridePerRequest() {
    InMemoryEventStore store = new InMemoryEventStore();
    DefaultOutboxEventPublisher publisher = withOverride(store, "B");

    java.util.List<UUID> ids =
        publisher.publishAll(
            java.util.List.of(
                new io.github.bams22.outboxer.api.publish.PublishRequest("A", "one", null),
                new io.github.bams22.outboxer.api.publish.PublishRequest("B", "two", null)));

    assertThat(store.findById(ids.get(0)).orElseThrow().payloadFormat())
        .isEqualTo(StringEventSerializer.FORMAT);
    assertThat(store.findById(ids.get(1)).orElseThrow().payloadFormat()).isEqualTo("test-alt");
  }

  private static final OutboxListener NOOP = new OutboxListener() {};

  /** Distinct-format stub so per-type routing is observable in the stored payloadFormat. */
  private static final io.github.bams22.outboxer.spi.EventSerializer ALT_SERIALIZER =
      new io.github.bams22.outboxer.spi.EventSerializer() {
        @Override
        public String format() {
          return "test-alt";
        }

        @Override
        public io.github.bams22.outboxer.domain.SerializedPayload serialize(Object payload) {
          return io.github.bams22.outboxer.domain.SerializedPayload.ofText("alt:" + payload);
        }

        @Override
        public <T> T deserialize(
            io.github.bams22.outboxer.domain.SerializedPayload payload, Class<T> type) {
          return type.cast(payload.requireText().substring("alt:".length()));
        }
      };

  private static DefaultOutboxEventPublisher withOverride(
      InMemoryEventStore store, String overriddenType) {
    return new DefaultOutboxEventPublisher(
        store,
        new StringEventSerializer(),
        java.util.Map.of(overriddenType, ALT_SERIALIZER),
        Clock.system(),
        TransactionContext.alwaysActive(),
        NoTransactionPolicy.FAIL,
        NOOP,
        PollerWaker.NOOP,
        io.github.bams22.outboxer.spi.OutboxTracer.NOOP);
  }

  private static DefaultOutboxEventPublisher plain(InMemoryEventStore store) {
    return new DefaultOutboxEventPublisher(
        store,
        new StringEventSerializer(),
        Clock.system(),
        TransactionContext.alwaysActive(),
        NoTransactionPolicy.FAIL,
        NOOP,
        PollerWaker.NOOP);
  }
}
