/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.ledger;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.LongAdder;

/**
 * Ledger for the in-process fleet: lock-free append, a set of succeeded sequence numbers for
 * progress polling. Costs the handler nothing measurable, so it does not distort the numbers.
 *
 * <p>The forked fleet (ADR-0034 phase 2) needs a cross-process ledger instead — a table in the
 * benchmark database — because handlers then run in other JVMs.
 */
public final class InMemoryLedger implements Ledger {

    private final ConcurrentLinkedQueue<Handling> entries = new ConcurrentLinkedQueue<>();
    private final Set<Long> succeeded = ConcurrentHashMap.newKeySet();
    private final LongAdder total = new LongAdder();

    @Override
    public void record(Handling handling) {
        Objects.requireNonNull(handling, "handling must not be null");
        entries.add(handling);
        total.increment();
        if (handling.succeeded()) {
            succeeded.add(handling.seq());
        }
    }

    @Override
    public long distinctSuccesses() {
        return succeeded.size();
    }

    @Override
    public long total() {
        return total.sum();
    }

    @Override
    public List<Handling> snapshot() {
        return new ArrayList<>(entries);
    }
}
