/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.tracing.micrometer;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.test.simple.SimpleTracer;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StoredTraceContextsTest {

    private static final String TRACE = "4bf92f3577b34da6a3ce929d0e0e4736";
    private static final String SPAN = "00f067aa0ba902b7";

    private final SimpleTracer tracer = new SimpleTracer();

    @Test
    void parsesW3cTraceparent() {
        TraceContext ctx =
                StoredTraceContexts.parse(
                        tracer, Map.of("traceparent", "00-" + TRACE + "-" + SPAN + "-01"));

        assertThat(ctx).isNotNull();
        assertThat(ctx.traceId()).isEqualTo(TRACE);
        assertThat(ctx.spanId()).isEqualTo(SPAN);
        assertThat(ctx.sampled()).isTrue();
    }

    @Test
    void readsTheSampledFlagFromTraceparent() {
        TraceContext ctx =
                StoredTraceContexts.parse(
                        tracer, Map.of("traceparent", "00-" + TRACE + "-" + SPAN + "-00"));

        assertThat(ctx).isNotNull();
        assertThat(ctx.sampled()).isFalse();
    }

    @Test
    void matchesKeysCaseInsensitively() {
        TraceContext ctx =
                StoredTraceContexts.parse(
                        tracer, Map.of("Traceparent", "00-" + TRACE + "-" + SPAN + "-01"));

        assertThat(ctx).isNotNull();
        assertThat(ctx.traceId()).isEqualTo(TRACE);
    }

    @Test
    void parsesSingleHeaderB3() {
        TraceContext ctx =
                StoredTraceContexts.parse(tracer, Map.of("b3", TRACE + "-" + SPAN + "-1"));

        assertThat(ctx).isNotNull();
        assertThat(ctx.traceId()).isEqualTo(TRACE);
        assertThat(ctx.spanId()).isEqualTo(SPAN);
        assertThat(ctx.sampled()).isTrue();
    }

    @Test
    void padsA64BitB3TraceIdTo128Bits() {
        TraceContext ctx =
                StoredTraceContexts.parse(
                        tracer, Map.of("b3", "a3ce929d0e0e4736-" + SPAN + "-0-" + SPAN));

        assertThat(ctx).isNotNull();
        assertThat(ctx.traceId()).isEqualTo("0000000000000000a3ce929d0e0e4736");
        assertThat(ctx.sampled()).isFalse();
    }

    @Test
    void parsesMultiHeaderB3() {
        TraceContext ctx =
                StoredTraceContexts.parse(
                        tracer,
                        Map.of(
                                "X-B3-TraceId", TRACE,
                                "X-B3-SpanId", SPAN,
                                "X-B3-Sampled", "1"));

        assertThat(ctx).isNotNull();
        assertThat(ctx.traceId()).isEqualTo(TRACE);
        assertThat(ctx.spanId()).isEqualTo(SPAN);
        assertThat(ctx.sampled()).isTrue();
    }

    @Test
    void prefersTraceparentWhenSeveralFormatsArePresent() {
        TraceContext ctx =
                StoredTraceContexts.parse(
                        tracer,
                        Map.of(
                                "traceparent",
                                "00-" + TRACE + "-" + SPAN + "-01",
                                "b3",
                                "ffffffffffffffffffffffffffffffff-eeeeeeeeeeeeeeee-1"));

        assertThat(ctx).isNotNull();
        assertThat(ctx.traceId()).isEqualTo(TRACE);
    }

    @Test
    void rejectsUnknownAndMalformedCarriers() {
        assertThat(StoredTraceContexts.parse(tracer, Map.of())).isNull();
        assertThat(StoredTraceContexts.parse(tracer, Map.of("uber-trace-id", "x:y:z:1"))).isNull();
        assertThat(StoredTraceContexts.parse(tracer, Map.of("traceparent", "garbage"))).isNull();
        assertThat(StoredTraceContexts.parse(tracer, Map.of("b3", TRACE))).isNull();
        assertThat(StoredTraceContexts.parse(tracer, Map.of("X-B3-TraceId", TRACE))).isNull();
    }

    @Test
    void rejectsAllZeroIds() {
        assertThat(
                        StoredTraceContexts.parse(
                                tracer,
                                Map.of(
                                        "traceparent",
                                        "00-00000000000000000000000000000000-" + SPAN + "-01")))
                .isNull();
        assertThat(
                        StoredTraceContexts.parse(
                                tracer,
                                Map.of("traceparent", "00-" + TRACE + "-0000000000000000-01")))
                .isNull();
    }
}
