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

import io.github.bams22.outboxer.spi.OutboxTraceAttributes;
import io.github.bams22.outboxer.spi.OutboxTracer;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.transport.Kind;
import io.micrometer.observation.transport.Propagator;
import io.micrometer.observation.transport.ReceiverContext;
import io.micrometer.observation.transport.SenderContext;
import io.micrometer.tracing.Tracer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.Nullable;

/**
 * {@link OutboxTracer} implementation on Micrometer's Observation API (ADR-0023).
 *
 * <p>Publish side: starts an observation over a {@link SenderContext} ({@link Kind#PRODUCER}),
 * which Boot's {@code PropagatingSenderTracingObservationHandler} turns into a producer span {@code
 * "outbox publish <eventType>"} and injects into the flat string carrier persisted in the event
 * row. The observation is started but never scoped — the caller's own context stays current.
 *
 * <p>Handle side: starts an observation over a {@link ReceiverContext} ({@link Kind#CONSUMER})
 * carrying the stored map, so {@code PropagatingReceiverTracingObservationHandler} extracts the
 * parent and creates the consumer span {@code "outbox process <eventType>"}. The observation scope
 * is opened on the worker thread and closed with the handle, which makes both the span and the
 * observation current — the latter is what {@code ContextPropagatingTaskDecorator} / {@code
 * ContextSnapshot} carry when handler code hops threads.
 *
 * <p>Going through the Observation API rather than {@link Tracer} directly is deliberate: the
 * registered {@code ObservationHandler}s own span kind, parent extraction, carrier injection and
 * error recording, the current observation propagates across thread hops, and the same instrument
 * also feeds meters (see below).
 *
 * <p>Behavioral notes vs the OpenTelemetry adapter ({@code event-outboxer-tracing-otel}):
 *
 * <ul>
 *   <li>Propagation format follows Spring Boot's {@code management.tracing.propagation.*} settings
 *       — the stored keys may be {@code b3} instead of {@code traceparent}. Values stay flat
 *       strings either way.
 *   <li>Baggage propagation follows the bridge's configuration ({@code
 *       management.tracing.baggage.remote-fields} on Boot).
 *   <li>Error semantics are bridge-dependent: the OTel bridge records an exception event plus ERROR
 *       status, the Brave bridge sets an error tag.
 *   <li>All span attributes are string tags, so {@code event_outboxer.attempt} appears as a string.
 *       The three {@code messaging.*} keys of the span are also timer tags (low-cardinality); ids,
 *       attempt and worker are high-cardinality (span-only).
 *   <li>Without tracing handlers on the registry (metrics-only setup, or {@link
 *       ObservationRegistry#NOOP}) there is no span and {@link PublishSpan#contextToStore()} is
 *       empty — the engine degrades to an untraced outbox hop.
 * </ul>
 *
 * <p><b>Meters produced as a side effect.</b> Boot's {@code DefaultMeterObservationHandler} turns
 * every observation into a {@code Timer} plus — while {@code
 * management.observations.long-task-timer.enabled} stays at its default {@code true} — a {@code
 * LongTaskTimer} named {@code <name>.active}. So wiring this adapter registers four meters: {@code
 * <prefix>.publish}, {@code <prefix>.publish.active}, {@code <prefix>.process} and {@code
 * <prefix>.process.active}. The timers carry the three low-cardinality {@code messaging.*} keys
 * plus Micrometer's own {@code error} tag ({@code none} or the exception's simple name). None of
 * them ship SLO histogram buckets — {@code META-INF/event-outboxer/metrics-defaults.yml} of {@code
 * event-outboxer-metrics-micrometer} covers only that module's timers — so they publish count/sum/
 * max and no {@code _bucket} series unless the application configures its own boundaries.
 *
 * <p>{@code <prefix>.process} is <em>not</em> a substitute for {@code
 * <prefix>.events.processing_time} of {@code event-outboxer-metrics-micrometer}: this observation
 * wraps only {@code EventHandler.handle(...)} and is recorded on every attempt including failures
 * and retries, whereas the listener's timer measures claim → finalize and only on success, and is
 * tagged {@code event_type}. Keep the listener's timer as the processing-latency SLI; suppress the
 * ones here with a {@code MeterFilter} if the extra meters are unwanted.
 *
 * <p>{@code <prefix>.publish} is only per-event latency for single {@code publish(...)} calls. The
 * batch path holds one handle per request open until the single {@code saveAll} returns, so one
 * sample covers the whole batch and the rest cover almost nothing — the distribution is an artefact
 * for applications that use {@code publishAll(...)}.
 *
 * <p><b>The event type becomes a real meter tag.</b> {@code messaging.destination.name} is a
 * low-cardinality key value, so each distinct event type creates its own timer and long-task timer.
 * This is the same assumption {@code MicrometerOutboxListener} already makes with its {@code
 * event_type} tag — event types must be a bounded, code-defined set, never a per-tenant or
 * per-entity string.
 *
 * <p><b>Observations can be switched off out from under the adapter.</b> Boot filters observations
 * by name through {@code management.observations.enable.*} (the lookup walks the name backwards on
 * dots). Setting {@code management.observations.enable.all=false}, or {@code
 * ...enable.<prefix>=false} to silence the meters above, makes {@link Observation#createNotStarted}
 * return a no-op: no spans and an empty stored context, so publish → handle trace continuity
 * disappears silently. Use a {@code MeterFilter} to drop the meters and leave the observations
 * alone.
 *
 * <p>Thread-safe; a single instance serves the publisher and every worker thread.
 */
public final class MicrometerOutboxTracer implements OutboxTracer {

    /**
     * Prefix applied to both observation names. Default: {@code event_outboxer} — the same default
     * {@code MicrometerOutboxListener} uses, so the meters these observations feed land in the same
     * namespace as the metrics module's. Configurable via the three-argument constructor; the
     * Spring Boot starter binds {@code event-outboxer.metrics.prefix} into the same slot.
     */
    public static final String DEFAULT_PREFIX = "event_outboxer";

    /**
     * Suffix of the publish-side observation name; the full name is {@code prefix + "." + this}.
     */
    public static final String PUBLISH_OBSERVATION_SUFFIX = "publish";

    /** Suffix of the handle-side observation name; the full name is {@code prefix + "." + this}. */
    public static final String PROCESS_OBSERVATION_SUFFIX = "process";

    private final ObservationRegistry observationRegistry;
    private final Tracer tracer;
    private final String publishObservationName;
    private final String processObservationName;

    public MicrometerOutboxTracer(ObservationRegistry observationRegistry, Tracer tracer) {
        this(observationRegistry, tracer, DEFAULT_PREFIX);
    }

    public MicrometerOutboxTracer(
            ObservationRegistry observationRegistry, Tracer tracer, String prefix) {
        this.observationRegistry =
                Objects.requireNonNull(observationRegistry, "observationRegistry must not be null");
        this.tracer = Objects.requireNonNull(tracer, "tracer must not be null");
        Objects.requireNonNull(prefix, "prefix must not be null");
        this.publishObservationName = prefix + "." + PUBLISH_OBSERVATION_SUFFIX;
        this.processObservationName = prefix + "." + PROCESS_OBSERVATION_SUFFIX;
    }

    /** Name of the publish-side observation — also the meter name of the timer it feeds. */
    public String publishObservationName() {
        return publishObservationName;
    }

    /** Name of the handle-side observation — also the meter name of the timer it feeds. */
    public String processObservationName() {
        return processObservationName;
    }

    @Override
    public PublishSpan startPublishSpan(UUID eventId, String eventType) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Map<String, String> carrier = new HashMap<>();
        SenderContext<Map<String, String>> context =
                new SenderContext<>(MapSetter.INSTANCE, Kind.PRODUCER);
        context.setCarrier(carrier);
        // Started, never scoped: the SPI forbids making anything current on the caller's thread.
        // The sender handler injects this span's context into the carrier during start().
        // The parent stays whatever is current on the caller's thread — that is the whole point of
        // the producer span, so unlike the handle side nothing is detached here.
        Observation observation =
                Observation.createNotStarted(
                                publishObservationName, () -> context, observationRegistry)
                        .contextualName("outbox publish " + eventType)
                        .lowCardinalityKeyValue(
                                OutboxTraceAttributes.MESSAGING_SYSTEM,
                                OutboxTraceAttributes.MESSAGING_SYSTEM_VALUE)
                        .lowCardinalityKeyValue(
                                OutboxTraceAttributes.MESSAGING_OPERATION_TYPE,
                                OutboxTraceAttributes.OPERATION_SEND)
                        .lowCardinalityKeyValue(
                                OutboxTraceAttributes.MESSAGING_DESTINATION_NAME, eventType)
                        .highCardinalityKeyValue(
                                OutboxTraceAttributes.MESSAGING_MESSAGE_ID, eventId.toString());
        // start() has meter side effects (a LongTaskTimer sample the registry retains until stop),
        // so a failure on the way out must not leave the observation in flight: SafeOutboxTracer
        // swallows what we rethrow and nothing would ever stop it.
        try {
            observation.start();
        } catch (RuntimeException ex) {
            stopQuietly(observation);
            throw ex;
        }
        return new MicrometerPublishSpan(observation, Map.copyOf(carrier));
    }

    @Override
    public ProcessSpan startProcessSpan(ProcessSpanInfo info) {
        Objects.requireNonNull(info, "info must not be null");
        ReceiverContext<Map<String, String>> context =
                new ReceiverContext<>(MapGetter.INSTANCE, Kind.CONSUMER);
        context.setCarrier(info.storedContext());
        Observation observation =
                Observation.createNotStarted(
                                processObservationName, () -> context, observationRegistry)
                        // createNotStarted already captured the thread's current observation as the
                        // parent; drop it for the same reason the span parent is detached below.
                        .parentObservation(null)
                        .contextualName("outbox process " + info.eventType())
                        .lowCardinalityKeyValue(
                                OutboxTraceAttributes.MESSAGING_SYSTEM,
                                OutboxTraceAttributes.MESSAGING_SYSTEM_VALUE)
                        .lowCardinalityKeyValue(
                                OutboxTraceAttributes.MESSAGING_OPERATION_TYPE,
                                OutboxTraceAttributes.OPERATION_PROCESS)
                        .lowCardinalityKeyValue(
                                OutboxTraceAttributes.MESSAGING_DESTINATION_NAME, info.eventType())
                        .highCardinalityKeyValue(
                                OutboxTraceAttributes.MESSAGING_MESSAGE_ID,
                                info.eventId().toString())
                        .highCardinalityKeyValue(
                                OutboxTraceAttributes.ATTEMPT, String.valueOf(info.attempt()))
                        .highCardinalityKeyValue(
                                OutboxTraceAttributes.WORKER_ID, info.workerId().value());
        // Detach the worker thread before the parent is resolved. Micrometer's Propagator.extract
        // falls back to the thread's current context on both bridges, so an ambient span (a leaked
        // scope, a decorated task) would silently adopt this consumer span. The parent must come
        // from the stored carrier or from nowhere — mirroring the OTel adapter's Context.root().
        try {
            try (Tracer.SpanInScope ignored = tracer.withSpan(null)) {
                observation.start();
            }
            return new MicrometerProcessSpan(observation, observation.openScope());
        } catch (RuntimeException ex) {
            // openScope() runs after start(): without this the observation stays in flight forever
            // and its LongTaskTimer sample never settles. See startPublishSpan.
            closeLeakedScope(observation);
            stopQuietly(observation);
            throw ex;
        }
    }

    /**
     * Releases the registry's thread-local when {@code openScope()} failed part-way. The scope
     * makes itself current before the handlers are notified, so a throwing {@code onScopeOpened}
     * leaves our observation current on a pooled worker thread with no scope object to close it.
     */
    private void closeLeakedScope(Observation observation) {
        Observation.Scope scope = observationRegistry.getCurrentObservationScope();
        if (scope == null || scope.getCurrentObservation() != observation) {
            return;
        }
        try {
            scope.close();
        } catch (RuntimeException ignored) {
            // close() notifies the same handlers that just failed; force the thread-local back.
            observationRegistry.setCurrentObservationScope(scope.getPreviousObservationScope());
        }
    }

    /**
     * Stops an observation that is being abandoned mid-construction, ignoring any secondary failure
     * — the caller is already unwinding with the original one.
     */
    private static void stopQuietly(Observation observation) {
        try {
            observation.stop();
        } catch (RuntimeException ignored) {
            // Best effort.
        }
    }

    @RequiredArgsConstructor
    private static final class MicrometerPublishSpan implements PublishSpan {

        private final Observation observation;
        private final Map<String, String> context;
        private final AtomicBoolean closed = new AtomicBoolean();

        @Override
        public Map<String, String> contextToStore() {
            return context;
        }

        @Override
        public void coalesced(UUID existingEventId) {
            observation.highCardinalityKeyValue(
                    OutboxTraceAttributes.COALESCED_INTO, existingEventId.toString());
        }

        @Override
        public void error(Throwable error) {
            observation.error(error);
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                observation.stop();
            }
        }
    }

    @RequiredArgsConstructor
    private static final class MicrometerProcessSpan implements ProcessSpan {

        private final Observation observation;
        private final Observation.Scope scope;
        private final AtomicBoolean closed = new AtomicBoolean();

        @Override
        public void error(Throwable error) {
            observation.error(error);
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                scope.close();
                observation.stop();
            }
        }
    }

    /** Writes the injected carrier entries into the map persisted with the event row. */
    private enum MapSetter implements Propagator.Setter<Map<String, String>> {
        INSTANCE;

        @Override
        public void set(@Nullable Map<String, String> carrier, String key, String value) {
            if (carrier != null) {
                carrier.put(key, value);
            }
        }
    }

    /** Reads the parent context back out of the map stored with the event row. */
    private enum MapGetter implements Propagator.Getter<Map<String, String>> {
        INSTANCE;

        @Override
        public @Nullable String get(@Nullable Map<String, String> carrier, String key) {
            return carrier == null ? null : carrier.get(key);
        }
    }
}
