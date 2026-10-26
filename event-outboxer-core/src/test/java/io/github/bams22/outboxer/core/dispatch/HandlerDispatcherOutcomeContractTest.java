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

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.api.handle.FailureDecision;
import io.github.bams22.outboxer.api.handle.FailureHandler;
import io.github.bams22.outboxer.api.handle.builtin.FailureHandlers;
import io.github.bams22.outboxer.api.handle.builtin.MaxRetriesFailureHandler.ExhaustedAction;
import io.github.bams22.outboxer.api.observer.EventDeletedInfo;
import io.github.bams22.outboxer.api.observer.EventDisabledInfo;
import io.github.bams22.outboxer.api.observer.EventRetryScheduledInfo;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.core.support.StringEventSerializer;
import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.EventType;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.spi.EventSerializerRegistry;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The two {@link EventOutcome} guarantees the dispatcher enforces independently of how the {@code
 * FailureHandler} chain is composed (ADR-0007, amendment 2026-10-26): an explicit {@code Fail} is
 * never retried, and a {@code Retry.delayOverride} re-times a retry without bypassing the chain's
 * decision whether to retry at all.
 */
class HandlerDispatcherOutcomeContractTest {

    private static final WorkerId WORKER = new WorkerId("outcome-contract-worker");
    private static final String TYPE = "CONTRACT";
    private static final Instant NOW = Instant.parse("2026-10-26T10:00:00Z");

    private final InMemoryEventStore store = new InMemoryEventStore();
    private final List<EventDisabledInfo> disabled = new ArrayList<>();
    private final List<EventRetryScheduledInfo> retries = new ArrayList<>();
    private final List<EventDeletedInfo> deleted = new ArrayList<>();

    // ---------------------------------------------------------------------------------------------
    // Fail is never retried
    // ---------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Fail under a chain that retries everything → DISABLED, not rescheduled")
    void failUnderRetryEverythingChainIsDisabled() {
        FailureHandler<String> retryEverything =
                ctx -> new FailureDecision.RetryAt(ctx.now().plusSeconds(60), "retry anyway");
        ClaimedEvent claimed = saveAndClaim();

        dispatcher(EventOutcome.fail("gone for good"), retryEverything).dispatch(claimed);

        Event after = store.findById(claimed.id()).orElseThrow();
        assertThat(after.status()).isEqualTo(EventStatus.DISABLED);
        assertThat(after.lastFailReason()).contains("gone for good").contains("explicit Fail");
        assertThat(retries).isEmpty();
        assertThat(disabled)
                .singleElement()
                .satisfies(
                        info -> {
                            assertThat(info.trigger())
                                    .isEqualTo(EventDisabledInfo.Trigger.FAILURE_DECISION);
                            assertThat(info.reason()).contains("explicit Fail");
                        });
    }

    @Test
    @DisplayName("Fail under a builder chain without withMaxAttempts → DISABLED")
    void failUnderBackoffOnlyChainIsDisabled() {
        FailureHandler<String> backoffOnly =
                FailureHandlers.<String>builder()
                        .withExponentialBackoff(
                                Duration.ofSeconds(5), 2.0, Duration.ofHours(1), 0.0);
        ClaimedEvent claimed = saveAndClaim();

        dispatcher(EventOutcome.fail("404"), backoffOnly).dispatch(claimed);

        assertThat(store.findById(claimed.id()).orElseThrow().status())
                .isEqualTo(EventStatus.DISABLED);
        assertThat(retries).isEmpty();
        assertThat(disabled).hasSize(1);
    }

    @Test
    @DisplayName("Fail under MaxRetries(DELETE) keeps the chain's choice → row deleted")
    void failUnderMaxRetriesDeleteIsDeleted() {
        FailureHandler<String> deleteOnExhaustion =
                FailureHandlers.<String>builder()
                        .withMaxAttempts(5, ExhaustedAction.DELETE)
                        .withFixedDelay(Duration.ofMinutes(1));
        ClaimedEvent claimed = saveAndClaim();

        dispatcher(EventOutcome.fail("unprocessable"), deleteOnExhaustion).dispatch(claimed);

        assertThat(store.findById(claimed.id())).isEmpty();
        assertThat(deleted)
                .singleElement()
                .extracting(EventDeletedInfo::reason)
                .asString()
                .contains("explicit Fail");
        assertThat(disabled).isEmpty();
        assertThat(retries).isEmpty();
    }

    // ---------------------------------------------------------------------------------------------
    // Retry.delayOverride re-times, never bypasses
    // ---------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Retry with delayOverride still hits the attempt cap")
    void retryWithDelayOverrideStillHitsAttemptCap() {
        FailureHandler<String> oneAttempt =
                FailureHandlers.<String>builder()
                        .withMaxAttempts(1, ExhaustedAction.DISABLE)
                        .withFixedDelay(Duration.ofHours(1));
        ClaimedEvent claimed = saveAndClaim();

        dispatcher(EventOutcome.retry("later", Duration.ofSeconds(5)), oneAttempt)
                .dispatch(claimed);

        Event after = store.findById(claimed.id()).orElseThrow();
        assertThat(after.status()).isEqualTo(EventStatus.DISABLED);
        assertThat(after.lastFailReason()).contains("max attempts (1) exhausted");
        assertThat(retries).isEmpty();
        assertThat(disabled).hasSize(1);
    }

    @Test
    @DisplayName("Retry with delayOverride under withNoRetry() → DISABLED")
    void retryWithDelayOverrideUnderNoRetryIsDisabled() {
        FailureHandler<String> noRetry = FailureHandlers.<String>builder().withNoRetry();
        ClaimedEvent claimed = saveAndClaim();

        dispatcher(EventOutcome.retry("later", Duration.ofSeconds(5)), noRetry).dispatch(claimed);

        assertThat(store.findById(claimed.id()).orElseThrow().status())
                .isEqualTo(EventStatus.DISABLED);
        assertThat(retries).isEmpty();
    }

    @Test
    @DisplayName("Retry with delayOverride re-times a custom chain's RetryAt to now + override")
    void retryWithDelayOverrideRetimesCustomChainDecision() {
        FailureHandler<String> anHourLater =
                ctx -> new FailureDecision.RetryAt(ctx.now().plus(Duration.ofHours(1)), "an hour");
        ClaimedEvent claimed = saveAndClaim();

        dispatcher(EventOutcome.retry("rate limited", Duration.ofSeconds(30)), anHourLater)
                .dispatch(claimed);

        Event after = store.findById(claimed.id()).orElseThrow();
        assertThat(after.status()).isEqualTo(EventStatus.PENDING);
        assertThat(after.attempts()).isEqualTo(1);
        assertThat(after.runAt()).isEqualTo(NOW.plusSeconds(30));
        assertThat(retries)
                .singleElement()
                .satisfies(
                        info -> {
                            assertThat(info.nextRunAt()).isEqualTo(NOW.plusSeconds(30));
                            assertThat(info.reason()).isEqualTo("an hour");
                        });
        assertThat(disabled).isEmpty();
    }

    @Test
    @DisplayName("Retry with delayOverride under the default chain uses the override, not backoff")
    void retryWithDelayOverrideUnderDefaultChainUsesTheOverride() {
        ClaimedEvent claimed = saveAndClaim();

        dispatcher(
                        EventOutcome.retry("rate limited", Duration.ofSeconds(30)),
                        FailureHandlers.defaults())
                .dispatch(claimed);

        Event after = store.findById(claimed.id()).orElseThrow();
        assertThat(after.status()).isEqualTo(EventStatus.PENDING);
        assertThat(after.runAt()).isEqualTo(NOW.plusSeconds(30));
    }

    @Test
    @DisplayName("Retry without delayOverride leaves the chain's timing alone")
    void retryWithoutDelayOverrideKeepsChainTiming() {
        FailureHandler<String> fixed =
                FailureHandlers.<String>builder().withFixedDelay(Duration.ofMinutes(2));
        ClaimedEvent claimed = saveAndClaim();

        dispatcher(EventOutcome.retry("later"), fixed).dispatch(claimed);

        assertThat(store.findById(claimed.id()).orElseThrow().runAt())
                .isEqualTo(NOW.plus(Duration.ofMinutes(2)));
    }

    // ---------------------------------------------------------------------------------------------
    // fixture
    // ---------------------------------------------------------------------------------------------

    private HandlerDispatcher dispatcher(EventOutcome outcome, FailureHandler<String> chain) {
        EventHandler<String> handler =
                new EventHandler<String>() {
                    @Override
                    public EventType<String> type() {
                        return EventType.of(TYPE, String.class);
                    }

                    @Override
                    public EventOutcome handle(EventContext ctx, String payload) {
                        return outcome;
                    }

                    @Override
                    public FailureHandler<String> failureHandler() {
                        return chain;
                    }
                };
        OutboxListener listener =
                new OutboxListener() {
                    @Override
                    public void onEventDisabled(EventDisabledInfo info) {
                        disabled.add(info);
                    }

                    @Override
                    public void onEventRetryScheduled(EventRetryScheduledInfo info) {
                        retries.add(info);
                    }

                    @Override
                    public void onEventDeleted(EventDeletedInfo info) {
                        deleted.add(info);
                    }
                };
        return HandlerDispatcher.builder()
                .store(store)
                .serializerRegistry(
                        EventSerializerRegistry.of(List.of(new StringEventSerializer())))
                .handlerResolver(new EventHandlerResolver(List.of(handler)))
                .listener(listener)
                .workerId(WORKER)
                .clock(() -> NOW)
                .build();
    }

    private ClaimedEvent saveAndClaim() {
        EventStore s = store;
        s.save(
                PendingEvent.builder()
                        .id(UUID.randomUUID())
                        .eventType(TYPE)
                        .payload(SerializedPayload.ofText("\"p\""))
                        .payloadFormat(StringEventSerializer.FORMAT)
                        .payloadClass("java.lang.String")
                        .priority((short) 0)
                        .runAt(Instant.now().minusSeconds(1))
                        .traceContext(Map.of())
                        .build());
        return s.claim(new ClaimRequest(TYPE, WORKER, 10)).get(0);
    }
}
