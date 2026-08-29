/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.publish;

import io.github.bams22.outboxer.api.observer.EventPublishedInfo;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.publish.OutboxEventPublisher;
import io.github.bams22.outboxer.api.publish.PublishOptions;
import io.github.bams22.outboxer.api.publish.PublishRequest;
import io.github.bams22.outboxer.core.polling.PollerWaker;
import io.github.bams22.outboxer.core.tracing.SafeOutboxTracer;
import io.github.bams22.outboxer.core.tracing.TracePropagationMarker;
import io.github.bams22.outboxer.domain.EventType;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.exception.NoTransactionException;
import io.github.bams22.outboxer.domain.exception.PublishFailedException;
import io.github.bams22.outboxer.domain.exception.PublishSerializationException;
import io.github.bams22.outboxer.domain.exception.PublishValidationException;
import io.github.bams22.outboxer.domain.exception.StorageException;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EventSerializer;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.OutboxTracer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import lombok.Builder;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default {@link OutboxEventPublisher} implementation. Holds no per-call state — a single instance
 * serves every thread. Steps:
 *
 * <ol>
 *   <li>Validate: non-null key and payload, and the payload is an instance of {@code
 *       type.payloadType()} (ADR-0031).
 *   <li>Consult the {@link TransactionContext}; if no active transaction, apply the configured
 *       {@link NoTransactionPolicy}.
 *   <li>Serialize the payload via {@link EventSerializer}.
 *   <li>Start a PRODUCER span via the {@link OutboxTracer} port and capture its trace context into
 *       the event, unless the caller supplied an explicit {@code PublishOptions.traceContext}
 *       override (ADR-0023).
 *   <li>Build a {@link PendingEvent} and hand it to {@link EventStore#save(PendingEvent)}.
 *   <li>Fire {@code OutboxListener.onEventPublished(...)}.
 *   <li>Register an after-commit hook that wakes the local poller of the published type, so
 *       same-JVM publish→handle latency is bounded by the handler, not by the poll interval.
 * </ol>
 *
 * <p><b>Construction.</b> {@code DefaultOutboxEventPublisher.builder()}. Required: {@code store}
 * and {@code serializer}. Every other knob has a default: no write-serializer overrides, {@link
 * Clock#system()}, {@link TransactionContext#alwaysActive()}, {@link NoTransactionPolicy#FAIL},
 * {@link OutboxListener#NOOP}, {@link PollerWaker#NOOP}, {@link OutboxTracer#NOOP}, {@link
 * #DEFAULT_DEFERRED_PROPAGATION} and {@link #DEFAULT_LINK_THRESHOLD}. {@code OutboxEngineBuilder}
 * passes every collaborator explicitly (and keeps its own {@code NoTransactionPolicy.IGNORE}
 * default); the builder defaults exist for standalone and test use.
 */
public final class DefaultOutboxEventPublisher implements OutboxEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(DefaultOutboxEventPublisher.class);

    private final EventStore store;
    private final EventSerializer serializer;
    private final Map<String, EventSerializer> writeSerializerOverrides;
    private final Clock clock;
    private final TransactionContext txContext;
    private final NoTransactionPolicy noTxPolicy;
    private final OutboxListener listener;
    private final PollerWaker waker;
    private final OutboxTracer tracer;
    private final OutboxTracer.Propagation deferredPropagation;
    private final Duration linkThreshold;

    /**
     * Default {@code deferredPropagation}: deferred events get a linked consumer span (ADR-0023,
     * 2026-08-28 amendment). {@link OutboxTracer.Propagation#CHILD} disables the rule.
     */
    public static final OutboxTracer.Propagation DEFAULT_DEFERRED_PROPAGATION =
            OutboxTracer.Propagation.LINK;

    /**
     * Default {@code linkThreshold}: an event scheduled more than one minute ahead gets a linked
     * consumer span instead of a child one (ADR-0023, 2026-08-28 amendment). One minute is
     * comfortably beyond every debounce-style {@code runAt} and beyond the decision window of
     * tail-based samplers, while anything a human would call "scheduled" clears it.
     */
    public static final Duration DEFAULT_LINK_THRESHOLD = Duration.ofMinutes(1);

    /**
     * Builder-backed constructor; parameter names are the builder's method names. Only {@code
     * store} and {@code serializer} are required — every other parameter accepts {@code null},
     * which selects the default listed in the class Javadoc.
     *
     * @param store event store the publisher inserts into
     * @param serializer default payload serializer
     * @param writeSerializerOverrides per-event-type write serializers (ADR-0025 amendment): events
     *     of a listed type are serialized with — and stamped with the {@code format()} of — the
     *     mapped serializer; {@code null} = none
     * @param clock publish-time clock; {@code null} = {@link Clock#system()}
     * @param transactionContext how the publisher detects the caller's transaction; {@code null} =
     *     {@link TransactionContext#alwaysActive()}
     * @param noTransactionPolicy what to do when no transaction is active; {@code null} = {@link
     *     NoTransactionPolicy#FAIL}
     * @param listener observer notified of publishes; {@code null} = {@link OutboxListener#NOOP}
     * @param waker after-commit poller wake-up; {@code null} = {@link PollerWaker#NOOP}
     * @param tracer tracing port (ADR-0023), wrapped defensively; {@code null} = {@link
     *     OutboxTracer#NOOP}
     * @param deferredPropagation span shape of a deferred event; {@code null} = {@link
     *     #DEFAULT_DEFERRED_PROPAGATION}
     * @param linkThreshold how far ahead {@code runAt} must lie for an event to count as deferred;
     *     {@code null} = {@link #DEFAULT_LINK_THRESHOLD}; must not be negative
     */
    @Builder
    private DefaultOutboxEventPublisher(
            EventStore store,
            EventSerializer serializer,
            @Nullable Map<String, EventSerializer> writeSerializerOverrides,
            @Nullable Clock clock,
            @Nullable TransactionContext transactionContext,
            @Nullable NoTransactionPolicy noTransactionPolicy,
            @Nullable OutboxListener listener,
            @Nullable PollerWaker waker,
            @Nullable OutboxTracer tracer,
            OutboxTracer.@Nullable Propagation deferredPropagation,
            @Nullable Duration linkThreshold) {
        this.store = Objects.requireNonNull(store, "store must not be null");
        this.serializer = Objects.requireNonNull(serializer, "serializer must not be null");
        this.writeSerializerOverrides =
                writeSerializerOverrides == null ? Map.of() : Map.copyOf(writeSerializerOverrides);
        this.clock = clock != null ? clock : Clock.system();
        this.txContext =
                transactionContext != null ? transactionContext : TransactionContext.alwaysActive();
        this.noTxPolicy =
                noTransactionPolicy != null ? noTransactionPolicy : NoTransactionPolicy.FAIL;
        this.listener = listener != null ? listener : OutboxListener.NOOP;
        this.waker = waker != null ? waker : PollerWaker.NOOP;
        this.tracer = SafeOutboxTracer.wrap(tracer != null ? tracer : OutboxTracer.NOOP);
        this.deferredPropagation =
                deferredPropagation != null ? deferredPropagation : DEFAULT_DEFERRED_PROPAGATION;
        if (linkThreshold != null && linkThreshold.isNegative()) {
            throw new IllegalArgumentException(
                    "linkThreshold must not be negative, got " + linkThreshold);
        }
        this.linkThreshold = linkThreshold != null ? linkThreshold : DEFAULT_LINK_THRESHOLD;
    }

    @Override
    public <T> UUID publish(EventType<T> type, T payload) {
        return publish(type, payload, PublishOptions.defaults());
    }

    @Override
    public <T> UUID publish(EventType<T> type, T payload, Instant runAt) {
        Objects.requireNonNull(runAt, "runAt must not be null");
        return publish(type, payload, PublishOptions.builder().runAt(runAt).build());
    }

    /**
     * Bounded retry for the coalescing race: the conditional insert conflicted, but the conflicting
     * PENDING row got claimed or finalized before we could lock it — re-insert.
     */
    private static final int DEDUP_RACE_RETRIES = 3;

    @Override
    public <T> UUID publish(EventType<T> type, T payload, @Nullable PublishOptions options) {
        validate(type, payload);
        String eventType = type.name();
        PublishOptions resolved = options == null ? PublishOptions.defaults() : options;
        enforceTransactionPolicy();

        // Serialization stays outside the span: a serialization failure is a caller bug, not a
        // messaging operation, and no event exists yet to trace.
        SerializedPayload serialized = serialize(eventType, payload);
        UUID id = UUID.randomUUID();
        try (OutboxTracer.PublishSpan span = tracer.startPublishSpan(id, eventType)) {
            PendingEvent pending = buildPending(id, eventType, payload, serialized, resolved, span);
            try {
                if (pending.dedupKey() != null) {
                    CoalescingResult result = saveCoalescing(pending);
                    if (!result.inserted()) {
                        // Coalesced into an existing PENDING event. No listener, no wake — nothing
                        // new was
                        // inserted, and the existing row is pinned (FOR UPDATE) inside this
                        // transaction, so
                        // claims skip it until we commit and its handler is guaranteed to see our
                        // changes.
                        UUID existingId =
                                Objects.requireNonNull(
                                        result.existingId(),
                                        "coalesced result must carry the existing event id");
                        span.coalesced(existingId);
                        return existingId;
                    }
                } else {
                    store.save(pending);
                }
            } catch (StorageException ex) {
                span.error(ex);
                throw new PublishFailedException(
                        "storage rejected event " + pending.id() + " of type " + eventType, ex);
            } catch (PublishFailedException ex) {
                span.error(ex);
                throw ex;
            }
            emitPublished(pending);
            scheduleWake(Set.of(eventType));
            return pending.id();
        }
    }

    private record CoalescingResult(boolean inserted, @Nullable UUID existingId) {}

    /**
     * ADR-0021 coalescing insert. Each loop iteration ends in exactly one of three outcomes:
     *
     * <ol>
     *   <li><b>Inserted</b> — no PENDING event with this {@code (type, key)} existed; the partial
     *       unique index arbitrated atomically ({@code ON CONFLICT}), which is why the insert goes
     *       FIRST: the common case (first publish of a key) resolves in a single statement, and a
     *       lock-first ordering would have the mirrored insert-after-check race anyway.
     *   <li><b>Coalesced</b> — the conflicting PENDING row was found and locked ({@code SELECT ...
     *       FOR UPDATE}) inside the caller's transaction. From this moment the claim query ({@code
     *       FOR UPDATE SKIP LOCKED}) skips the row until our commit, so its handler is guaranteed
     *       to observe this transaction's changes. If the lock lands on a DIFFERENT row than the
     *       one that caused the conflict (old one finalized, another publisher inserted anew), that
     *       is equally correct: any pinned PENDING event of this key carries the work and runs
     *       after our commit.
     *   <li><b>Vanished</b> — the row that conflicted with our insert is no longer PENDING by the
     *       time we try to lock it (claimed, possibly already finalized). This is not a missed race
     *       but the semantically REQUIRED branch: an event claimed before our commit may run
     *       against a snapshot without our changes, so coalescing into it would lose our update —
     *       we must loop and insert our own event (the old row no longer occupies the
     *       PENDING-scoped unique index, so the retry succeeds unless yet another publisher got
     *       there first).
     * </ol>
     *
     * <p>The lock probe cannot read stale state: if the row is being claimed concurrently, our
     * {@code SELECT ... FOR UPDATE} blocks on the claim's row lock and PostgreSQL re-evaluates the
     * {@code status = 'PENDING'} predicate against the committed row version afterwards
     * (EvalPlanQual) — we either hold the lock on a genuinely PENDING row or see empty.
     *
     * <p>Reaching the retry bound requires a fresh PENDING row of the same key to appear AND
     * disappear in the microsecond window between our conflict and our lock, {@code
     * DEDUP_RACE_RETRIES} times in a row — a pathological churn that the bound converts from a
     * theoretical livelock into a loud failure.
     */
    private CoalescingResult saveCoalescing(PendingEvent pending) {
        String dedupKey =
                Objects.requireNonNull(pending.dedupKey(), "saveCoalescing requires a dedupKey");
        for (int attempt = 0; attempt < DEDUP_RACE_RETRIES; attempt++) {
            if (store.save(pending)) {
                return new CoalescingResult(true, null);
            }
            var existing = store.lockPendingByDedupKey(pending.eventType(), dedupKey);
            if (existing.isPresent()) {
                return new CoalescingResult(false, existing.get());
            }
            // The PENDING row vanished under us — loop and insert our own.
        }
        throw new PublishFailedException(
                "could not publish event with dedupKey '"
                        + dedupKey
                        + "' of type "
                        + pending.eventType()
                        + " after "
                        + DEDUP_RACE_RETRIES
                        + " attempts (pathological claim/finalize churn on the key)",
                null);
    }

    @Override
    public List<UUID> publishAll(Collection<? extends PublishRequest<?>> requests) {
        Objects.requireNonNull(requests, "requests must not be null");
        if (requests.isEmpty()) {
            return List.of();
        }
        enforceTransactionPolicy();

        // Requests with a dedup key need per-row coalescing feedback and go through save(...) one
        // by one; the rest batch through saveAll. Returned ids stay aligned with request order.
        List<PendingEvent> batch = new ArrayList<>(requests.size());
        List<UUID> ids = new ArrayList<>(requests.size());
        List<PendingEvent> inserted = new ArrayList<>(requests.size());
        // PRODUCER spans of batch-path events stay open until saveAll below actually inserts them;
        // dedup-path spans close per row inside the loop.
        List<OutboxTracer.PublishSpan> batchSpans = new ArrayList<>(requests.size());
        try {
            for (PublishRequest<?> r : requests) {
                Objects.requireNonNull(r, "request element must not be null");
                validate(r.type(), r.payload());
                String eventType = r.type().name();
                PublishOptions opts = r.options() == null ? PublishOptions.defaults() : r.options();
                SerializedPayload serialized = serialize(eventType, r.payload());
                UUID id = UUID.randomUUID();
                if (opts.dedupKey() != null) {
                    try (OutboxTracer.PublishSpan span = tracer.startPublishSpan(id, eventType)) {
                        PendingEvent pe =
                                buildPending(id, eventType, r.payload(), serialized, opts, span);
                        try {
                            CoalescingResult result = saveCoalescing(pe);
                            if (result.inserted()) {
                                inserted.add(pe);
                                ids.add(pe.id());
                            } else {
                                UUID existingId =
                                        Objects.requireNonNull(
                                                result.existingId(),
                                                "coalesced result must carry the existing event"
                                                        + " id");
                                span.coalesced(existingId);
                                ids.add(existingId);
                            }
                        } catch (StorageException | PublishFailedException ex) {
                            span.error(ex);
                            throw ex;
                        }
                    }
                } else {
                    OutboxTracer.PublishSpan span = tracer.startPublishSpan(id, eventType);
                    batchSpans.add(span);
                    PendingEvent pe =
                            buildPending(id, eventType, r.payload(), serialized, opts, span);
                    batch.add(pe);
                    ids.add(pe.id());
                }
            }
            store.saveAll(batch);
            inserted.addAll(batch);
        } catch (StorageException ex) {
            for (OutboxTracer.PublishSpan span : batchSpans) {
                span.error(ex);
            }
            throw new PublishFailedException("publishAll(" + requests.size() + ") failed", ex);
        } finally {
            for (OutboxTracer.PublishSpan span : batchSpans) {
                span.close();
            }
        }
        for (PendingEvent pe : inserted) {
            emitPublished(pe);
        }
        Set<String> types = new LinkedHashSet<>();
        for (PendingEvent pe : inserted) {
            types.add(pe.eventType());
        }
        scheduleWake(types);
        return List.copyOf(ids);
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    /**
     * API-boundary validation: parameters are declared {@code @Nullable} on purpose — the public
     * {@code publish} methods promise non-null inputs, but callers without JSpecify tooling can
     * still pass null, and this check turns that into a {@link PublishValidationException} instead
     * of an unexplained NPE.
     */
    private void validate(@Nullable EventType<?> type, @Nullable Object payload) {
        if (type == null) {
            throw new PublishValidationException("type must not be null");
        }
        if (payload == null) {
            throw new PublishValidationException("payload must not be null");
        }
        if (!type.payloadType().isInstance(payload)) {
            throw new PublishValidationException(
                    "payload of event type "
                            + type.name()
                            + " must be an instance of "
                            + type.payloadType().getName()
                            + ", got "
                            + payload.getClass().getName());
        }
    }

    private void enforceTransactionPolicy() {
        if (txContext.isActive()) {
            return;
        }
        if (noTxPolicy == NoTransactionPolicy.FAIL) {
            throw new NoTransactionException(
                    "OutboxEventPublisher.publish must be invoked inside a transaction "
                            + "(configure noTransactionPolicy=IGNORE to opt out).");
        }
    }

    private EventSerializer serializerFor(String eventType) {
        return writeSerializerOverrides.getOrDefault(eventType, serializer);
    }

    private SerializedPayload serialize(String eventType, Object payload) {
        try {
            return serializerFor(eventType).serialize(payload);
        } catch (PublishSerializationException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            throw new PublishSerializationException(
                    "failed to serialize payload of type " + payload.getClass().getName(), ex);
        }
    }

    /**
     * Builds the row to insert. Resolves {@code runAt}, the carrier to store and the propagation
     * decision (ADR-0023). An explicit {@code PublishOptions.traceContext} override always wins
     * over the ambient context captured by the producer span; either way, an event scheduled
     * further ahead than the link threshold is stored with the {@code link} marker and the producer
     * span is told about it (2026-08-28 amendment). The decision is made here, once, against the
     * publish-time clock — retries and backlog never revisit it.
     */
    private PendingEvent buildPending(
            UUID id,
            String eventType,
            Object payload,
            SerializedPayload serialized,
            PublishOptions options,
            OutboxTracer.PublishSpan span) {
        Instant now = clock.now();
        Instant runAt = options.runAt() != null ? options.runAt() : now;
        short priority = options.priority() != null ? options.priority() : (short) 0;
        Map<String, String> explicit = options.traceContext();
        Map<String, String> traceContext = explicit != null ? explicit : span.contextToStore();
        if (isDeferredBeyondLinkThreshold(runAt, now)) {
            span.linked();
            traceContext = TracePropagationMarker.markLinked(traceContext);
        }
        return PendingEvent.builder()
                .id(id)
                .eventType(eventType)
                .payload(serialized)
                .payloadFormat(serializerFor(eventType).format())
                .payloadClass(payload.getClass().getName())
                .priority(priority)
                .runAt(runAt)
                .traceContext(traceContext)
                .dedupKey(options.dedupKey())
                .build();
    }

    private boolean isDeferredBeyondLinkThreshold(Instant runAt, Instant now) {
        return deferredPropagation == OutboxTracer.Propagation.LINK
                && Duration.between(now, runAt).compareTo(linkThreshold) > 0;
    }

    private void emitPublished(PendingEvent pe) {
        listener.onEventPublished(
                new EventPublishedInfo(
                        pe.id(), pe.eventType(), clock.now(), pe.runAt(), pe.priority()));
    }

    /**
     * Wake the local pollers of the published types once the surrounding transaction commits.
     * Purely an optimization — every path is swallowed on failure so a wake can never break the
     * caller's commit.
     */
    private void scheduleWake(Set<String> eventTypes) {
        try {
            txContext.afterCommit(
                    () -> {
                        for (String type : eventTypes) {
                            try {
                                waker.wake(type);
                            } catch (RuntimeException ex) {
                                log.debug(
                                        "poller wake failed for type {}: {}", type, ex.toString());
                            }
                        }
                    });
        } catch (RuntimeException ex) {
            log.debug("afterCommit registration failed: {}", ex.toString());
        }
    }
}
