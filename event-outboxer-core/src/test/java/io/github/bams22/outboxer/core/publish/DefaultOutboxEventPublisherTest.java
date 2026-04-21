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
            });

    UUID id = publisher.publish("T", "hello");

    Optional<Event> saved = store.findById(id);
    assertThat(saved).isPresent();
    assertThat(saved.orElseThrow().payload()).isEqualTo("hello");
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
            NOOP);

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
            NOOP);

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
  void publishIfAbsentIsUnsupportedInMvp() {
    DefaultOutboxEventPublisher publisher = plain(new InMemoryEventStore());
    assertThatThrownBy(
            () -> publisher.publishIfAbsent("T", "hello", "idemp", PublishOptions.defaults()))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  private static final OutboxListener NOOP = new OutboxListener() {};

  private static DefaultOutboxEventPublisher plain(InMemoryEventStore store) {
    return new DefaultOutboxEventPublisher(
        store,
        new StringEventSerializer(),
        Clock.system(),
        TransactionContext.alwaysActive(),
        NoTransactionPolicy.FAIL,
        NOOP);
  }
}
