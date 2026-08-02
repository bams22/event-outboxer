/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.polling;

import static org.assertj.core.api.Assertions.assertThatCode;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PollerWakeHubTest {

    @Test
    @DisplayName("waking an unknown event type is a silent no-op")
    void unknownTypeIsNoOp() {
        PollerWakeHub hub = new PollerWakeHub();

        assertThatCode(() -> hub.wake("NOBODY_POLLS_THIS")).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("NOOP waker never throws")
    void noopWakerIsSafe() {
        assertThatCode(() -> PollerWaker.NOOP.wake("ANY")).doesNotThrowAnyException();
    }
}
