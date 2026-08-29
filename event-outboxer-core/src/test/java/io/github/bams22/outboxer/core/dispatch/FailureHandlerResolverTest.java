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
import io.github.bams22.outboxer.api.handle.FailureDecision;
import io.github.bams22.outboxer.api.handle.FailureHandler;
import io.github.bams22.outboxer.domain.EventType;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Resolution order of the failure chain (ADR-0007, ADR-0030): the handler's own override, then the
 * per-type registration, then the default.
 */
class FailureHandlerResolverTest {

    private static final FailureHandler<Object> DEFAULT =
            ctx -> new FailureDecision.Disable("default");
    private static final FailureHandler<Object> PER_TYPE =
            ctx -> new FailureDecision.Disable("per-type");
    private static final FailureHandler<String> ON_HANDLER =
            ctx -> new FailureDecision.Disable("on-handler");

    private final FailureHandlerResolver resolver =
            new FailureHandlerResolver(Map.of("ORDER", PER_TYPE), DEFAULT);

    @Test
    @DisplayName("EventHandler.failureHandler() wins over the per-type registration")
    void handlerOverrideWins() {
        assertThat(resolver.resolve(handler("ORDER", ON_HANDLER))).isSameAs(ON_HANDLER);
    }

    @Test
    @DisplayName("the per-type registration wins over the default")
    void perTypeWins() {
        assertThat(resolver.resolve(handler("ORDER", null))).isSameAs(PER_TYPE);
    }

    @Test
    @DisplayName("an unregistered type falls back to the default")
    void unknownTypeFallsBack() {
        assertThat(resolver.resolve(handler("OTHER", null))).isSameAs(DEFAULT);
    }

    @Test
    @DisplayName("null arguments are rejected")
    void nullGuards() {
        assertThatThrownBy(() -> new FailureHandlerResolver(null, DEFAULT))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new FailureHandlerResolver(Map.of(), null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> resolver.resolve(null)).isInstanceOf(NullPointerException.class);
    }

    private static EventHandler<String> handler(
            String type, @Nullable FailureHandler<String> failureHandler) {
        return new EventHandler<>() {
            @Override
            public EventType<String> type() {
                return EventType.of(type, String.class);
            }

            @Override
            public EventOutcome handle(EventContext ctx, String payload) {
                return EventOutcome.success();
            }

            @Override
            public @Nullable FailureHandler<String> failureHandler() {
                return failureHandler;
            }
        };
    }
}
