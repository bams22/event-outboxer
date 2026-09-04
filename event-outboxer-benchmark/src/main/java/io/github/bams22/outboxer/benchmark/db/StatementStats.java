/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.db;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Calls and rows per statement class from {@code pg_stat_statements}, for the outbox schema's own
 * statements. Two samples bracket a run; {@link #minus} gives what the run executed. The classes
 * are recognised by the shape of the SQL the PostgreSQL adapter issues:
 *
 * <ul>
 *   <li>{@code claim} — {@code WITH picked AS (...) UPDATE ... RETURNING}
 *   <li>{@code insert} — {@code INSERT INTO <schema>.events}
 *   <li>{@code finalizeBatch} — the group-commit multi-row {@code DELETE ... USING (VALUES ...)}
 *       (or the archive CTE)
 *   <li>{@code finalizeSingle} — the single-row {@code DELETE ... WHERE id = $1 AND version = $2}
 *   <li>{@code release} — {@code UPDATE ... SET status = 'PENDING'} (lock busy, finalize failure)
 *   <li>{@code other} — everything else against the schema (heartbeats, maintenance, leases)
 * </ul>
 *
 * @param calls statement executions per class
 * @param rows rows affected or returned per class
 */
public record StatementStats(Map<String, Long> calls, Map<String, Long> rows) {

    public StatementStats {
        calls = Map.copyOf(Objects.requireNonNull(calls, "calls must not be null"));
        rows = Map.copyOf(Objects.requireNonNull(rows, "rows must not be null"));
    }

    /** Class-wise difference {@code this - earlier}; classes absent earlier count from zero. */
    public StatementStats minus(StatementStats earlier) {
        Map<String, Long> c = new TreeMap<>();
        Map<String, Long> r = new TreeMap<>();
        for (String k : calls.keySet()) {
            c.put(k, calls.get(k) - earlier.calls.getOrDefault(k, 0L));
            r.put(k, rows.getOrDefault(k, 0L) - earlier.rows.getOrDefault(k, 0L));
        }
        return new StatementStats(c, r);
    }

    /** All calls together. */
    public long totalCalls() {
        return calls.values().stream().mapToLong(Long::longValue).sum();
    }

    /** Calls of one class, zero when absent. */
    public long calls(String cls) {
        return calls.getOrDefault(cls, 0L);
    }

    /** Rows per call of one class, zero when the class did not run. */
    public double rowsPerCall(String cls) {
        long c = calls(cls);
        return c == 0 ? 0 : (double) rows.getOrDefault(cls, 0L) / c;
    }
}
