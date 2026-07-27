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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.domain.exception.DuplicateHandlerException;
import java.util.List;
import org.junit.jupiter.api.Test;

class EventHandlerResolverTest {

  @Test
  void lookupByType() {
    EventHandlerResolver r =
        new EventHandlerResolver(List.of(stringHandler("A"), stringHandler("B")));
    assertThat(r.find("A")).isPresent();
    assertThat(r.find("B")).isPresent();
    assertThat(r.find("C")).isEmpty();
    assertThat(r.registeredTypes()).containsExactlyInAnyOrder("A", "B");
  }

  @Test
  void rejectsDuplicates() {
    assertThatThrownBy(
            () -> new EventHandlerResolver(List.of(stringHandler("DUP"), stringHandler("DUP"))))
        .isInstanceOf(DuplicateHandlerException.class)
        .hasMessageContaining("DUP");
  }

  private static EventHandler<String> stringHandler(String type) {
    return new EventHandler<String>() {
      @Override
      public String eventType() {
        return type;
      }

      @Override
      public Class<String> payloadType() {
        return String.class;
      }

      @Override
      public EventOutcome handle(EventContext ctx, String payload) {
        return EventOutcome.Success.INSTANCE;
      }
    };
  }
}
