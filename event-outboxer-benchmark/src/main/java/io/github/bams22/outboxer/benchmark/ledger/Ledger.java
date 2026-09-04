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

import java.util.List;

/**
 * Where the target's handler reports every invocation. Implementations must be safe for concurrent
 * {@link #record} calls from many handler threads; reads happen from the driver thread.
 */
public interface Ledger {

    /** Appends one handling. Never throws for a well-formed entry. */
    void record(Handling handling);

    /**
     * Number of distinct sequence numbers with at least one successful handling — the drain
     * progress counter.
     */
    long distinctSuccesses();

    /** Total invocations recorded so far, all outcomes. */
    long total();

    /**
     * A consistent-enough copy for grading after the drain. Not intended to be called while
     * handlers are still running.
     */
    List<Handling> snapshot();
}
