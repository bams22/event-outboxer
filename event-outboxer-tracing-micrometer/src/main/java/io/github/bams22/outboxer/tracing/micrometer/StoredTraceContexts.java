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

import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;

/**
 * Reads the trace and span id of the stored producer context straight out of the carrier, for use
 * as a span-link target (ADR-0023, 2026-08-28 amendment). Understands the three formats Spring Boot
 * can be configured to propagate ({@code management.tracing.propagation.type}): W3C {@code
 * traceparent}, single-header {@code b3} and multi-header {@code X-B3-*}. Keys are matched
 * case-insensitively; a 64-bit B3 trace id is left-padded to 128 bits the way OpenTelemetry's own
 * B3 propagator does. Anything else — or an all-zero id — yields {@code null}.
 */
final class StoredTraceContexts {

    private static final Pattern TRACEPARENT =
            Pattern.compile("^[0-9a-f]{2}-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$");

    private static final Pattern B3_SINGLE =
            Pattern.compile(
                    "^([0-9a-f]{16}|[0-9a-f]{32})-([0-9a-f]{16})(?:-([01d]))?(?:-[0-9a-f]{16})?$");

    private static final Pattern HEX_ID = Pattern.compile("^([0-9a-f]{16}|[0-9a-f]{32})$");

    private static final Pattern ZEROS = Pattern.compile("^0+$");

    private StoredTraceContexts() {}

    /**
     * Returns the parsed context, or {@code null} when the carrier holds no recognisable trace
     * identifiers.
     */
    static @Nullable TraceContext parse(Tracer tracer, Map<String, String> carrier) {
        String traceparent = get(carrier, "traceparent");
        if (traceparent != null) {
            Matcher m = TRACEPARENT.matcher(traceparent.trim().toLowerCase(Locale.ROOT));
            if (m.matches()) {
                boolean sampled = (Integer.parseInt(m.group(3), 16) & 0x01) != 0;
                return build(tracer, m.group(1), m.group(2), sampled);
            }
        }
        String b3 = get(carrier, "b3");
        if (b3 != null) {
            Matcher m = B3_SINGLE.matcher(b3.trim().toLowerCase(Locale.ROOT));
            if (m.matches()) {
                String flag = m.group(3);
                Boolean sampled = flag == null ? null : !"0".equals(flag);
                return build(tracer, m.group(1), m.group(2), sampled);
            }
        }
        String traceId = get(carrier, "X-B3-TraceId");
        String spanId = get(carrier, "X-B3-SpanId");
        if (traceId != null && spanId != null) {
            String t = traceId.trim().toLowerCase(Locale.ROOT);
            String s = spanId.trim().toLowerCase(Locale.ROOT);
            if (HEX_ID.matcher(t).matches() && s.length() == 16 && HEX_ID.matcher(s).matches()) {
                String sampledHeader = get(carrier, "X-B3-Sampled");
                Boolean sampled =
                        sampledHeader == null
                                ? null
                                : "1".equals(sampledHeader.trim())
                                        || "true".equalsIgnoreCase(sampledHeader.trim())
                                        || "d".equalsIgnoreCase(sampledHeader.trim());
                return build(tracer, t, s, sampled);
            }
        }
        return null;
    }

    private static @Nullable TraceContext build(
            Tracer tracer, String traceId, String spanId, @Nullable Boolean sampled) {
        if (ZEROS.matcher(traceId).matches() || ZEROS.matcher(spanId).matches()) {
            return null;
        }
        String padded = traceId.length() == 32 ? traceId : "0".repeat(16) + traceId;
        return tracer.traceContextBuilder().traceId(padded).spanId(spanId).sampled(sampled).build();
    }

    private static @Nullable String get(Map<String, String> carrier, String key) {
        String exact = carrier.get(key);
        if (exact != null) {
            return exact;
        }
        for (Map.Entry<String, String> e : carrier.entrySet()) {
            if (e.getKey().equalsIgnoreCase(key)) {
                return e.getValue();
            }
        }
        return null;
    }
}
