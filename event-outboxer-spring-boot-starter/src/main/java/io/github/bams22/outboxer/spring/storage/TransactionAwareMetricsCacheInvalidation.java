/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.storage;

import io.github.bams22.outboxer.core.publish.TransactionContext;
import io.github.bams22.outboxer.spi.MetricsSnapshotCache;
import io.github.bams22.outboxer.spi.OutboxMetricsSnapshot;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link MetricsSnapshotCache} decorator that holds an {@link #invalidate()} until the caller's
 * transaction commits. Wrapped around the cache handed to {@code PostgresOutboxAdmin}; the {@code
 * EventStore} gets the undecorated bean, since {@link #get()} and {@link #put} have nothing to
 * defer.
 *
 * <h2>Why</h2>
 *
 * The admin runs its statements on the caller's connection (ADR-0002). Inside a {@code
 * @Transactional} method the re-enabled or purged row becomes visible to other sessions at commit,
 * not when the {@code UPDATE} ran — so invalidating at statement time opens a window in which a
 * concurrent scrape computes pre-mutation counts and caches them. The snapshot's {@code takenAt}
 * barrier in {@link MetricsSnapshotCache} cannot close this one: such a scrape starts <em>after</em>
 * the invalidation, so its snapshot is legitimately newer and gets stored. Deferring the
 * invalidation to after-commit is what orders the two.
 *
 * <p>Without a Spring-managed transaction the statement has already auto-committed and {@link
 * TransactionContext#afterCommit(Runnable)} runs the invalidation straight away. A rollback drops
 * it entirely, which is correct: nothing changed. Several mutations in one transaction each
 * register their own after-commit invalidation — idempotent, and one cheap call apiece.
 *
 * <p>The delegate's failure never reaches the caller: a mutation that already committed must not be
 * reported as failed because a cache backend was unreachable, and at after-commit time there is no
 * caller left to report to anyway.
 */
final class TransactionAwareMetricsCacheInvalidation implements MetricsSnapshotCache {

    private static final Logger log =
            LoggerFactory.getLogger(TransactionAwareMetricsCacheInvalidation.class);

    private final MetricsSnapshotCache delegate;
    private final TransactionContext transactions;

    TransactionAwareMetricsCacheInvalidation(
            MetricsSnapshotCache delegate, TransactionContext transactions) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        this.transactions = Objects.requireNonNull(transactions, "transactions must not be null");
    }

    @Override
    public Optional<OutboxMetricsSnapshot> get() {
        return delegate.get();
    }

    @Override
    public void put(OutboxMetricsSnapshot snapshot) {
        delegate.put(snapshot);
    }

    @Override
    public void invalidate() {
        transactions.afterCommit(this::invalidateNow);
    }

    private void invalidateNow() {
        try {
            delegate.invalidate();
        } catch (RuntimeException ex) {
            log.warn(
                    "metrics snapshot cache invalidation failed; backlog gauges may stay stale for"
                            + " up to one metrics-cache-ttl: {}",
                    ex.toString());
        }
    }
}
