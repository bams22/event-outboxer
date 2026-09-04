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

/**
 * Row-level write counters of one schema, summed over its tables from {@code pg_stat_user_tables}.
 * Two samples bracket a run; their difference divided by events is the "database cost" figure.
 *
 * @param inserts {@code n_tup_ins}
 * @param updates {@code n_tup_upd} (HOT updates included)
 * @param deletes {@code n_tup_del}
 */
public record TableWrites(long inserts, long updates, long deletes) {

    /** All row writes together. */
    public long total() {
        return inserts + updates + deletes;
    }

    /** Counter-wise difference {@code this - earlier}. */
    public TableWrites minus(TableWrites earlier) {
        return new TableWrites(
                inserts - earlier.inserts, updates - earlier.updates, deletes - earlier.deletes);
    }
}
