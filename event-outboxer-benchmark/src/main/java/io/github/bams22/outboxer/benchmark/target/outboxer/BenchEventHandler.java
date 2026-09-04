/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.target.outboxer;

import io.github.bams22.outboxer.api.handle.EventContext;
import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.handle.EventOutcome;
import io.github.bams22.outboxer.benchmark.ledger.Handling;
import io.github.bams22.outboxer.benchmark.ledger.Ledger;
import io.github.bams22.outboxer.benchmark.scenario.Scenario;
import io.github.bams22.outboxer.domain.EventType;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import org.jspecify.annotations.Nullable;

/**
 * The handler every worker context registers, one instance per event type. It does the scenario's
 * simulated work, injects the scenario's failures on first attempts, and writes the invocation to
 * the ledger <em>before</em> returning, so the ledger entry precedes the library's own finalize.
 *
 * <p>Failure injection is deterministic in {@code seq}: the same event fails in every run with the
 * same rate, and only on attempt 1, so that a {@code retry} never turns into a lost event through
 * the retry policy's attempt cap.
 */
public final class BenchEventHandler<T> implements EventHandler<T> {

    private static final String INJECTED = "bench: injected failure on first attempt";

    private final EventType<T> type;
    private final Ledger ledger;
    private final Duration workTime;
    private final int failPerMille;
    private final ToLongFunction<T> seqOf;
    private final Function<T, @Nullable String> lockKeyOf;

    /**
     * @param seqOf reads the sequence number out of the payload shape
     * @param lockKeyOf reads the lock key, {@code null} for none
     */
    public BenchEventHandler(
            EventType<T> type,
            Ledger ledger,
            Scenario scenario,
            ToLongFunction<T> seqOf,
            Function<T, @Nullable String> lockKeyOf) {
        this.type = Objects.requireNonNull(type, "type must not be null");
        this.ledger = Objects.requireNonNull(ledger, "ledger must not be null");
        Objects.requireNonNull(scenario, "scenario must not be null");
        this.workTime = scenario.handlerWorkTime();
        this.failPerMille = (int) Math.round(scenario.failureRate() * 1000);
        this.seqOf = Objects.requireNonNull(seqOf, "seqOf must not be null");
        this.lockKeyOf = Objects.requireNonNull(lockKeyOf, "lockKeyOf must not be null");
    }

    @Override
    public EventType<T> type() {
        return type;
    }

    @Override
    public @Nullable String extractLockKey(T payload) {
        return lockKeyOf.apply(payload);
    }

    @Override
    public EventOutcome handle(EventContext ctx, T payload) {
        long seq = seqOf.applyAsLong(payload);
        Instant started = Instant.now();
        Handling.Outcome verdict;
        if (ctx.attempt() == 1 && injectFailure(seq)) {
            verdict = Handling.Outcome.RETRY;
        } else {
            simulateWork();
            verdict = Handling.Outcome.SUCCESS;
        }
        ledger.record(
                Handling.builder()
                        .seq(seq)
                        .eventType(ctx.eventType())
                        .attempt(ctx.attempt())
                        .workerId(ctx.workerId().value())
                        .thread(threadLabel())
                        .lockKey(lockKeyOf.apply(payload))
                        .startedAt(started)
                        .finishedAt(Instant.now())
                        .outcome(verdict)
                        .build());
        return verdict == Handling.Outcome.SUCCESS
                ? EventOutcome.success()
                : EventOutcome.retry(INJECTED);
    }

    private boolean injectFailure(long seq) {
        if (failPerMille == 0) {
            return false;
        }
        // Fibonacci hashing spreads consecutive seqs; floorMod keeps the bucket non-negative.
        long bucket = Math.floorMod((seq + 1) * 0x9E3779B97F4A7C15L >>> 20, 1000L);
        return bucket < failPerMille;
    }

    private void simulateWork() {
        if (workTime.isZero()) {
            return;
        }
        try {
            Thread.sleep(workTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Name plus id: executor thread names repeat across the per-type executors of one worker
     * ({@code outbox-handler-1} exists in every pool), so the name alone undercounts threads.
     */
    private static String threadLabel() {
        Thread t = Thread.currentThread();
        String name = t.getName();
        return (name.isBlank() ? "thread" : name) + "#" + t.threadId();
    }
}
