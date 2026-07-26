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

import io.github.bams22.outboxer.api.handle.builtin.FailureHandlers;
import io.github.bams22.outboxer.core.config.EventTypeConfig;
import io.github.bams22.outboxer.core.config.EventTypeConfigProvider;
import io.github.bams22.outboxer.core.support.StringEventSerializer;
import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Direct coverage of {@link HandlerDispatcher#releaseRejected}. With capacity-aware claiming
 * the poller almost never triggers real dispatch rejections any more, so the release path needs
 * a unit-level test of its own: the row must return to PENDING without consuming an attempt.
 */
class HandlerDispatcherReleaseRejectedTest {

  private static final WorkerId WORKER = new WorkerId("release-test-worker");

  @Test
  @DisplayName("releaseRejected returns the row to PENDING without burning an attempt")
  void releasesWithoutAttemptsBump() {
    InMemoryEventStore store = new InMemoryEventStore();
    HandlerDispatcher dispatcher =
        new HandlerDispatcher(
            store,
            EntityLocker.NOOP,
            new StringEventSerializer(),
            new EventHandlerResolver(List.of()),
            new FailureHandlerResolver(Map.of(), FailureHandlers.defaults()),
            new InFlightRegistry(),
            new io.github.bams22.outboxer.api.observer.OutboxListener() {},
            Clock.system(),
            WORKER,
            new EventTypeConfigProvider(EventTypeConfig.defaults(), Map.of()),
            DispatcherConfig.defaults());

    store.save(
        PendingEvent.builder()
            .id(UUID.randomUUID())
            .eventType("REJ")
            .payload("\"p\"")
            .payloadClass("java.lang.String")
            .priority((short) 0)
            .runAt(Instant.now().minusSeconds(1))
            .traceContext(Map.of())
            .build());
    ClaimedEvent claimed = store.claim(new ClaimRequest("REJ", WORKER, 10)).get(0);

    dispatcher.releaseRejected(claimed);

    Event after = store.findById(claimed.id()).orElseThrow();
    assertThat(after.status()).isEqualTo(EventStatus.PENDING);
    assertThat(after.claimedBy()).isNull();
    assertThat(after.attempts()).isEqualTo(claimed.attempts());
    assertThat(after.version()).isGreaterThan(claimed.claimedVersion());
    assertThat(after.runAt()).isAfterOrEqualTo(Instant.now().minusSeconds(1));
  }
}
