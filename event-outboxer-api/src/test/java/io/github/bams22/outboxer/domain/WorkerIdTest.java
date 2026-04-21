/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class WorkerIdTest {

  @Test
  void rejectsNullValue() {
    assertThatThrownBy(() -> new WorkerId(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void rejectsBlankValue() {
    assertThatThrownBy(() -> new WorkerId("   "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("blank");
  }

  @Test
  void rejectsOverlyLongValue() {
    String tooLong = "x".repeat(WorkerId.MAX_LENGTH + 1);
    assertThatThrownBy(() -> new WorkerId(tooLong))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("64");
  }

  @Test
  void acceptsBoundaryLength() {
    String boundary = "x".repeat(WorkerId.MAX_LENGTH);
    assertThat(new WorkerId(boundary).value()).hasSize(WorkerId.MAX_LENGTH);
  }

  @Test
  void generateDefaultProducesValidWorkerId() {
    WorkerId id = WorkerId.generateDefault();
    assertThat(id.value()).isNotBlank().hasSizeLessThanOrEqualTo(WorkerId.MAX_LENGTH);
    // Hostname-PID-uuid8 → at least two dashes.
    assertThat(id.value().chars().filter(c -> c == '-').count()).isGreaterThanOrEqualTo(2L);
  }

  @Test
  void twoGenerateDefaultsDifferByUuidComponent() {
    WorkerId a = WorkerId.generateDefault();
    WorkerId b = WorkerId.generateDefault();
    assertThat(a).isNotEqualTo(b);
  }
}
