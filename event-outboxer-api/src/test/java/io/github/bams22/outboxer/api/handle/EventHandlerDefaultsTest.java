/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.api.handle;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class EventHandlerDefaultsTest {

  private static final EventHandler<String> MINIMAL =
      new EventHandler<>() {
        @Override
        public String eventType() {
          return "TEST";
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

  @Test
  void extractLockKeyDefaultsToNull() {
    assertThat(MINIMAL.extractLockKey("anything")).isNull();
  }

  @Test
  void failureHandlerDefaultsToNull() {
    assertThat(MINIMAL.failureHandler()).isNull();
  }
}
