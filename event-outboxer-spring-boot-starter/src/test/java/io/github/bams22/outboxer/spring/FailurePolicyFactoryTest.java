/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.api.handle.FailureContext;
import io.github.bams22.outboxer.api.handle.FailureDecision;
import io.github.bams22.outboxer.api.handle.FailureHandler;
import io.github.bams22.outboxer.api.handle.builtin.LogFailureHandler;
import io.github.bams22.outboxer.api.handle.builtin.MaxRetriesFailureHandler.ExhaustedAction;
import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.exception.InvariantViolationException;
import io.github.bams22.outboxer.spring.OutboxProperties.FailureStrategy;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;
import org.springframework.boot.logging.LogLevel;

/**
 * The failure-policy thin merge (ADR-0030): per-type override → YAML defaults → library chain,
 * validated per layer with the offending property path in the message.
 */
class FailurePolicyFactoryTest {

    private static final Instant NOW = Instant.parse("2026-08-29T12:00:00Z");

    @Test
    @DisplayName("nothing set → no chain built, the engine keeps FailureHandlers.defaults()")
    void unsetLeavesLibraryDefaults() {
        OutboxProperties.EventTypes types = new OutboxProperties.EventTypes();
        types.getOverrides().put("ORDER", new OutboxProperties.EventType()); // no failure.* keys

        assertThat(FailurePolicyFactory.defaultChain(types)).isNull();
        assertThat(FailurePolicyFactory.perTypeChains(types)).isEmpty();
    }

    @Test
    @DisplayName("defaults with only max-attempts keep the exponential leaf and the log decorator")
    void defaultsThinMerge() {
        OutboxProperties.EventTypes types = new OutboxProperties.EventTypes();
        types.getDefaults().getFailure().setMaxAttempts(5);

        FailureHandler<?> chain = FailurePolicyFactory.defaultChain(types);

        assertThat(chain).isInstanceOf(LogFailureHandler.class);
        assertThat(decide(chain, 1))
                .isInstanceOfSatisfying(
                        FailureDecision.RetryAt.class,
                        retry ->
                                assertThat(retry.when())
                                        .isBetween(
                                                NOW.plusSeconds(4), NOW.plusSeconds(6))); // 5s ±20%
        assertThat(decide(chain, 5)).isInstanceOf(FailureDecision.Disable.class);
    }

    @Test
    @DisplayName("a per-type override layers on the YAML defaults, not on the library defaults")
    void overrideLayersOnDefaults() {
        OutboxProperties.EventTypes types = new OutboxProperties.EventTypes();
        types.getDefaults().getFailure().setMaxAttempts(5);
        OutboxProperties.EventType sendEmail = new OutboxProperties.EventType();
        sendEmail.getFailure().setStrategy(FailureStrategy.fixed); // fixed-delay left at 30s
        types.getOverrides().put("SEND_EMAIL", sendEmail);

        Map<String, FailureHandler<?>> chains = FailurePolicyFactory.perTypeChains(types);

        assertThat(chains).containsOnlyKeys("SEND_EMAIL");
        FailureHandler<?> chain = chains.get("SEND_EMAIL");
        assertThat(decide(chain, 1))
                .isInstanceOfSatisfying(
                        FailureDecision.RetryAt.class,
                        retry -> assertThat(retry.when()).isEqualTo(NOW.plusSeconds(30)));
        assertThat(decide(chain, 5)).isInstanceOf(FailureDecision.Disable.class); // from defaults
    }

    @Test
    @DisplayName("strategy none disables on the first failure; exhausted-action DELETE deletes")
    void noneAndDelete() {
        OutboxProperties.EventTypes types = new OutboxProperties.EventTypes();
        types.getDefaults().getFailure().setStrategy(FailureStrategy.none);
        assertThat(decide(FailurePolicyFactory.defaultChain(types), 1))
                .isInstanceOf(FailureDecision.Disable.class);

        OutboxProperties.EventTypes deleting = new OutboxProperties.EventTypes();
        deleting.getDefaults().getFailure().setMaxAttempts(1);
        deleting.getDefaults().getFailure().setExhaustedAction(ExhaustedAction.DELETE);
        assertThat(decide(FailurePolicyFactory.defaultChain(deleting), 1))
                .isInstanceOf(FailureDecision.Delete.class);
    }

    @Test
    @DisplayName("log-level OFF drops the logging decorator; FATAL maps to ERROR")
    void logLevels() {
        OutboxProperties.EventTypes off = new OutboxProperties.EventTypes();
        off.getDefaults().getFailure().setLogLevel(LogLevel.OFF);
        assertThat(FailurePolicyFactory.defaultChain(off)).isNotInstanceOf(LogFailureHandler.class);

        OutboxProperties.EventTypes fatal = new OutboxProperties.EventTypes();
        fatal.getDefaults().getFailure().setLogLevel(LogLevel.FATAL);
        assertThat(FailurePolicyFactory.defaultChain(fatal))
                .isInstanceOfSatisfying(
                        LogFailureHandler.class,
                        log -> assertThat(log.level()).isEqualTo(Level.ERROR));
    }

    @Test
    @DisplayName("a bad value fails naming the property the user wrote")
    void validationNamesTheProperty() {
        OutboxProperties.EventTypes types = new OutboxProperties.EventTypes();
        OutboxProperties.EventType sendEmail = new OutboxProperties.EventType();
        sendEmail.getFailure().setMultiplier(1.0);
        types.getOverrides().put("SEND_EMAIL", sendEmail);

        assertThatThrownBy(() -> FailurePolicyFactory.perTypeChains(types))
                .isInstanceOf(InvariantViolationException.class)
                .hasMessageContaining(
                        "event-outboxer.event-types.overrides.SEND_EMAIL.failure.multiplier")
                .hasMessageContaining("> 1.0");
    }

    @Test
    @DisplayName("the cross-field rule is checked on the merged policy and names both keys")
    void crossFieldAfterMerge() {
        OutboxProperties.EventTypes types = new OutboxProperties.EventTypes();
        types.getDefaults().getFailure().setBaseDelay(Duration.ofSeconds(5));
        OutboxProperties.EventType sendEmail = new OutboxProperties.EventType();
        sendEmail.getFailure().setMaxDelay(Duration.ofSeconds(1));
        types.getOverrides().put("SEND_EMAIL", sendEmail);

        assertThatThrownBy(() -> FailurePolicyFactory.perTypeChains(types))
                .isInstanceOf(InvariantViolationException.class)
                .hasMessageContaining("SEND_EMAIL.failure.max-delay")
                .hasMessageContaining("SEND_EMAIL.failure.base-delay");
    }

    @SuppressWarnings("unchecked")
    private static FailureDecision decide(FailureHandler<?> chain, int attempt) {
        ClaimedEvent event =
                new ClaimedEvent(
                        UUID.randomUUID(),
                        "TEST",
                        SerializedPayload.ofText("{}"),
                        "test-json",
                        "java.lang.String",
                        (short) 0,
                        attempt,
                        NOW.minusSeconds(10),
                        NOW.minusSeconds(1),
                        Map.of(),
                        1L);
        FailureContext<Object> ctx =
                new FailureContext<>(
                        event,
                        "payload",
                        new EventOutcome.Retry("boom", null, null),
                        null,
                        attempt,
                        NOW);
        return ((FailureHandler<Object>) chain).onFailure(ctx);
    }
}
