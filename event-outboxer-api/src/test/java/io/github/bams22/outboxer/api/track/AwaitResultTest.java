/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.api.track;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.SerializedPayload;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class AwaitResultTest {

    private static final UUID ID = UUID.randomUUID();

    @Test
    void completedAllowsMissingArchiveRowButNotMissingId() {
        AwaitResult completed = new AwaitResult.Completed(ID, null);
        assertThat(completed.eventId()).isEqualTo(ID);
        assertThat(completed.isCompleted()).isTrue();
        assertThat(completed.isDisabled()).isFalse();
        assertThat(completed.isTimedOut()).isFalse();

        assertThatThrownBy(() -> new AwaitResult.Completed(null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("eventId");
    }

    @Test
    void disabledTakesItsIdFromTheEvent() {
        AwaitResult disabled = new AwaitResult.Disabled(disabledEvent());
        assertThat(disabled.eventId()).isEqualTo(ID);
        assertThat(disabled.isDisabled()).isTrue();

        assertThatThrownBy(() -> new AwaitResult.Disabled(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("event");
    }

    @Test
    void timedOutValidatesItsComponents() {
        AwaitResult timedOut =
                new AwaitResult.TimedOut(ID, TrackedState.PENDING, Duration.ofMillis(50));
        assertThat(timedOut.isTimedOut()).isTrue();

        assertThatThrownBy(
                        () -> new AwaitResult.TimedOut(null, TrackedState.PENDING, Duration.ZERO))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new AwaitResult.TimedOut(ID, null, Duration.ZERO))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new AwaitResult.TimedOut(ID, TrackedState.PENDING, null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(
                        () ->
                                new AwaitResult.TimedOut(
                                        ID, TrackedState.PENDING, Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("waited");
    }

    @Test
    void sealedSwitchIsExhaustiveWithoutDefault() {
        assertThat(describe(new AwaitResult.Completed(ID, null))).isEqualTo("completed");
        assertThat(describe(new AwaitResult.Disabled(disabledEvent()))).isEqualTo("disabled: boom");
        assertThat(describe(new AwaitResult.TimedOut(ID, TrackedState.PROCESSING, Duration.ZERO)))
                .isEqualTo("timed out in PROCESSING");
    }

    private static String describe(AwaitResult result) {
        return switch (result) {
            case AwaitResult.Completed c -> "completed";
            case AwaitResult.Disabled d -> "disabled: " + d.event().lastFailReason();
            case AwaitResult.TimedOut t -> "timed out in " + t.lastSeen();
        };
    }

    private static Event disabledEvent() {
        Instant now = Instant.now();
        return Event.builder()
                .id(ID)
                .eventType("T")
                .payload(SerializedPayload.ofText("p"))
                .payloadFormat("text")
                .payloadClass("java.lang.String")
                .attempts(3)
                .status(EventStatus.DISABLED)
                .createdAt(now)
                .runAt(now)
                .lastFailReason("boom")
                .traceContext(Map.of())
                .version(4)
                .build();
    }
}
