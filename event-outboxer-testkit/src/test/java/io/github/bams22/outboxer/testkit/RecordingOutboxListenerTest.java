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

import io.github.bams22.outboxer.api.observer.EventCoalescedInfo;
import io.github.bams22.outboxer.api.observer.HandlerAbandonedInfo;
import io.github.bams22.outboxer.api.observer.MaintenanceRunInfo;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.domain.WorkerId;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RecordingOutboxListenerTest {

    @Test
    @DisplayName("every OutboxListener callback has a recording override — none silently dropped")
    void overridesEveryCallback() {
        List<String> notRecorded = new ArrayList<>();
        for (Method callback : OutboxListener.class.getDeclaredMethods()) {
            if (!callback.isDefault()) {
                continue;
            }
            try {
                RecordingOutboxListener.class.getDeclaredMethod(
                        callback.getName(), callback.getParameterTypes());
            } catch (NoSuchMethodException e) {
                notRecorded.add(callback.getName());
            }
        }
        assertThat(notRecorded).isEmpty();
    }

    @Test
    @DisplayName("clear() resets every captured list, handlersAbandoned included")
    void clearResetsEverything() {
        RecordingOutboxListener listener = new RecordingOutboxListener();
        listener.onEventCoalesced(new EventCoalescedInfo(UUID.randomUUID(), "T", "k"));
        listener.onMaintenanceRunCompleted(
                new MaintenanceRunInfo("heartbeat", MaintenanceRunInfo.Result.OK, null));
        listener.onHandlerAbandoned(
                new HandlerAbandonedInfo(
                        UUID.randomUUID(),
                        "T",
                        new WorkerId("w-1"),
                        "outbox-T-1",
                        Duration.ofMinutes(6),
                        true));
        assertThat(listener.coalesced()).hasSize(1);
        assertThat(listener.maintenanceRuns()).hasSize(1);
        assertThat(listener.handlersAbandoned()).hasSize(1);

        listener.clear();

        assertThat(listener.coalesced()).isEmpty();
        assertThat(listener.maintenanceRuns()).isEmpty();
        assertThat(listener.handlersAbandoned()).isEmpty();
    }
}
