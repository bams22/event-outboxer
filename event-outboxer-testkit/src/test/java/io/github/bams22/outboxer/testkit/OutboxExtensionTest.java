/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(OutboxExtension.class)
class OutboxExtensionTest {

  @Test
  void injectsContextParameter(OutboxTestContext ctx) {
    assertThat(ctx).isNotNull();
    assertThat(ctx.publisher()).isNotNull();
    assertThat(ctx.manualEngine()).isNotNull();
    assertThat(ctx.eventStore()).isNotNull();
  }

  @Test
  void eachTestReceivesSameContextWithinMethod(OutboxTestContext ctx) {
    assertThat(ctx).isNotNull();
  }
}
