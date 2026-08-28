/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bams22.outboxer.spi.OutboxTracer;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TracePropagationMarkerTest {

    private static final Map<String, String> CARRIER =
            Map.of("traceparent", "00-11111111111111111111111111111111-2222222222222222-01");

    @Test
    void keyIsTheSpanAttributeName() {
        assertThat(TracePropagationMarker.KEY).isEqualTo("event_outboxer.propagation");
    }

    @Test
    void markLinkedAddsTheMarkerToACopy() {
        Map<String, String> marked = TracePropagationMarker.markLinked(CARRIER);

        assertThat(marked)
                .containsAllEntriesOf(CARRIER)
                .containsEntry("event_outboxer.propagation", "link")
                .hasSize(2);
        assertThat(CARRIER).hasSize(1);
        assertThatThrownBy(() -> marked.put("x", "y"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void markLinkedLeavesAnEmptyCarrierEmpty() {
        assertThat(TracePropagationMarker.markLinked(Map.of())).isEmpty();
    }

    @Test
    void propagationOfReadsTheMarker() {
        assertThat(TracePropagationMarker.propagationOf(CARRIER))
                .isEqualTo(OutboxTracer.Propagation.CHILD);
        assertThat(TracePropagationMarker.propagationOf(TracePropagationMarker.markLinked(CARRIER)))
                .isEqualTo(OutboxTracer.Propagation.LINK);
        assertThat(
                        TracePropagationMarker.propagationOf(
                                Map.of("event_outboxer.propagation", "child")))
                .isEqualTo(OutboxTracer.Propagation.CHILD);
    }

    @Test
    void stripRemovesOnlyTheMarker() {
        Map<String, String> marked = TracePropagationMarker.markLinked(CARRIER);

        assertThat(TracePropagationMarker.strip(marked)).isEqualTo(CARRIER);
        assertThat(TracePropagationMarker.strip(CARRIER)).isSameAs(CARRIER);
    }
}
