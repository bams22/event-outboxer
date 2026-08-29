/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.polling;

import java.util.concurrent.Executor;

/**
 * The poller's view of a per-type handler executor: submission plus the two capacity signals that
 * couple claiming to processing. The poller claims {@code min(claimBatchSize, freeCapacity())}, and
 * only once {@code freeCapacity() >= claimMinFree}, so it never claims rows it cannot dispatch
 * (avoiding claim/release churn under overload) and can refill a prefetch queue in bulk; {@link
 * #onCapacityAvailable(Runnable)} wakes it the moment free capacity reaches that threshold (instead
 * of sleeping out the poll interval).
 */
public interface HandlerExecutorGate extends Executor {

    /**
     * Number of tasks this executor can accept right now without rejecting. A soft signal: it may
     * race with concurrent completions and submissions — under-reads cost one extra poll round,
     * over-reads are caught by the {@code RejectedExecutionException} safety net. Returns zero
     * while the engine is stopped.
     */
    int freeCapacity();

    /**
     * Register the single listener invoked when free capacity reaches the type's {@code
     * claimMinFree} refill threshold — with the default of 1, the transition from saturated (zero
     * free capacity) back to having room. Deliberately edge-triggered — firing on every task
     * completion would trigger one claim query per completion under load instead of letting free
     * slots accumulate into a batch.
     */
    void onCapacityAvailable(Runnable callback);
}
