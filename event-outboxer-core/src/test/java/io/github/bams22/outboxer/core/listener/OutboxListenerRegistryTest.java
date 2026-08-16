/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.listener;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.api.observer.EventPublishedInfo;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class OutboxListenerRegistryTest {

    @Test
    void broadcastsToAllListeners() {
        AtomicInteger a = new AtomicInteger();
        AtomicInteger b = new AtomicInteger();
        OutboxListenerRegistry registry = new OutboxListenerRegistry();
        registry.add(
                new OutboxListener() {
                    @Override
                    public void onEventPublished(EventPublishedInfo info) {
                        a.incrementAndGet();
                    }
                });
        registry.add(
                new OutboxListener() {
                    @Override
                    public void onEventPublished(EventPublishedInfo info) {
                        b.incrementAndGet();
                    }
                });

        registry.onEventPublished(somePublished());

        assertThat(a).hasValue(1);
        assertThat(b).hasValue(1);
    }

    @Test
    void oneFailingListenerDoesNotBlockOthers() {
        AtomicInteger a = new AtomicInteger();
        OutboxListenerRegistry registry = new OutboxListenerRegistry();
        registry.add(
                new OutboxListener() {
                    @Override
                    public void onEventPublished(EventPublishedInfo info) {
                        throw new RuntimeException("boom");
                    }
                });
        registry.add(
                new OutboxListener() {
                    @Override
                    public void onEventPublished(EventPublishedInfo info) {
                        a.incrementAndGet();
                    }
                });

        registry.onEventPublished(somePublished());

        assertThat(a).hasValue(1);
    }

    @Test
    @DisplayName("every listener callback is fanned out — an inherited default silently drops it")
    void overridesEveryCallback() {
        List<String> notFannedOut = new ArrayList<>();
        for (Method callback : OutboxListener.class.getDeclaredMethods()) {
            if (!callback.isDefault()) {
                continue;
            }
            try {
                // getDeclaredMethod, not getMethod: inheriting the interface's no-op default is
                // exactly the bug this guards against.
                OutboxListenerRegistry.class.getDeclaredMethod(
                        callback.getName(), callback.getParameterTypes());
            } catch (NoSuchMethodException e) {
                notFannedOut.add(callback.getName());
            }
        }
        assertThat(notFannedOut).isEmpty();
    }

    private static EventPublishedInfo somePublished() {
        Instant now = Instant.now();
        return new EventPublishedInfo(UUID.randomUUID(), "T", now, now, (short) 0);
    }
}
