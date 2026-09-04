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

import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.publish.OutboxEventPublisher;
import io.github.bams22.outboxer.benchmark.ledger.Ledger;
import io.github.bams22.outboxer.benchmark.scenario.PayloadFormat;
import io.github.bams22.outboxer.benchmark.scenario.Scenario;
import io.github.bams22.outboxer.benchmark.target.BenchmarkEvent;
import io.github.bams22.outboxer.benchmark.target.outboxer.proto.BenchPayloadProto;
import io.github.bams22.outboxer.domain.EventType;
import java.util.List;
import java.util.stream.IntStream;

/**
 * The one place that knows both payload shapes: the Jackson record {@link BenchPayload} and the
 * protoc-generated {@link BenchPayloadProto}. Event types are {@code BENCH_0 .. BENCH_{n-1}} with
 * the payload class of the scenario's format; handlers read {@code seq} and {@code lockKey} back
 * through the accessors here.
 */
final class BenchPayloads {

    private static final String PREFIX = "BENCH_";

    private BenchPayloads() {}

    /**
     * All event types for {@code count} types in {@code format}, index-aligned with {@code
     * Scenario.typeIndexFor}.
     */
    static List<EventType<?>> types(int count, PayloadFormat format) {
        return IntStream.range(0, count)
                .<EventType<?>>mapToObj(
                        i ->
                                format == PayloadFormat.PROTOBUF
                                        ? EventType.of(PREFIX + i, BenchPayloadProto.class)
                                        : EventType.of(PREFIX + i, BenchPayload.class))
                .toList();
    }

    /** The handler for {@code type}, reporting into {@code ledger}. */
    static EventHandler<?> handler(EventType<?> type, Ledger ledger, Scenario scenario) {
        if (type.payloadType() == BenchPayloadProto.class) {
            @SuppressWarnings("unchecked")
            EventType<BenchPayloadProto> proto = (EventType<BenchPayloadProto>) type;
            return new BenchEventHandler<>(
                    proto,
                    ledger,
                    scenario,
                    BenchPayloadProto::getSeq,
                    p -> p.getLockKey().isEmpty() ? null : p.getLockKey());
        }
        @SuppressWarnings("unchecked")
        EventType<BenchPayload> record = (EventType<BenchPayload>) type;
        return new BenchEventHandler<>(
                record, ledger, scenario, BenchPayload::seq, BenchPayload::lockKey);
    }

    /** Publishes {@code event} as the payload shape of {@code type}. */
    static void publish(OutboxEventPublisher publisher, EventType<?> type, BenchmarkEvent event) {
        if (type.payloadType() == BenchPayloadProto.class) {
            @SuppressWarnings("unchecked")
            EventType<BenchPayloadProto> proto = (EventType<BenchPayloadProto>) type;
            publisher.publish(
                    proto,
                    BenchPayloadProto.newBuilder()
                            .setSeq(event.seq())
                            .setLockKey(event.lockKey() == null ? "" : event.lockKey())
                            .setPadding(event.padding())
                            .build());
            return;
        }
        @SuppressWarnings("unchecked")
        EventType<BenchPayload> record = (EventType<BenchPayload>) type;
        publisher.publish(record, new BenchPayload(event.seq(), event.lockKey(), event.padding()));
    }
}
