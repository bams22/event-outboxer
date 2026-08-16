/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.maintenance;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.observer.StaleClaimsSweptInfo;
import io.github.bams22.outboxer.core.support.ForwardingEventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class StaleClaimSweeperTaskTest {

    private static final Duration THRESHOLD = Duration.ofMinutes(10);

    @Test
    @DisplayName("sweeps in batches until a short batch and reports the total to the listener")
    void sweepsUntilShortBatchAndNotifiesListener() {
        // Scripted sweep results: two full batches of 5 followed by a short batch of 2.
        Deque<Integer> sweeps = new ArrayDeque<>(List.of(5, 5, 2));
        ForwardingEventStore store =
                new ForwardingEventStore(new InMemoryEventStore()) {
                    @Override
                    public int sweepStale(Duration olderThan, int limit) {
                        return sweeps.isEmpty() ? 0 : sweeps.pop();
                    }
                };
        List<StaleClaimsSweptInfo> sweptInfos = new ArrayList<>();
        OutboxListener listener =
                new OutboxListener() {
                    @Override
                    public void onStaleClaimsSwept(StaleClaimsSweptInfo info) {
                        sweptInfos.add(info);
                    }
                };
        StaleClaimSweeperTask task =
                new StaleClaimSweeperTask(store, listener, THRESHOLD, Duration.ofMinutes(1), 5);

        task.run();

        assertThat(sweptInfos).containsExactly(new StaleClaimsSweptInfo(12, THRESHOLD));
    }

    @Test
    @DisplayName("a clean pass (nothing swept) does not fire the listener")
    void quietPassDoesNotNotify() {
        List<StaleClaimsSweptInfo> sweptInfos = new ArrayList<>();
        OutboxListener listener =
                new OutboxListener() {
                    @Override
                    public void onStaleClaimsSwept(StaleClaimsSweptInfo info) {
                        sweptInfos.add(info);
                    }
                };
        StaleClaimSweeperTask task =
                new StaleClaimSweeperTask(
                        new InMemoryEventStore(), listener, THRESHOLD, Duration.ofMinutes(1), 5);

        task.run();

        assertThat(sweptInfos).isEmpty();
    }
}
