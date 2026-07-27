/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.support;

import io.github.bams22.outboxer.spi.OutboxTracer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Recording {@link OutboxTracer} test double. Every started span is retained with its inputs,
 * error, coalesce target, and close count; publish spans hand out a unique canned {@code
 * traceparent} so tests can assert exactly which span's context landed on which event row.
 */
public final class RecordingOutboxTracer implements OutboxTracer {

  public final List<RecordedPublishSpan> publishSpans = new CopyOnWriteArrayList<>();
  public final List<RecordedProcessSpan> processSpans = new CopyOnWriteArrayList<>();

  /** When set, span starts throw — exercises the SafeOutboxTracer degradation path. */
  public volatile boolean throwOnStart;

  private final AtomicInteger seq = new AtomicInteger();

  @Override
  public PublishSpan startPublishSpan(UUID eventId, String eventType) {
    if (throwOnStart) {
      throw new IllegalStateException("tracer exploded on startPublishSpan");
    }
    RecordedPublishSpan span =
        new RecordedPublishSpan(eventId, eventType, cannedContext(seq.incrementAndGet()));
    publishSpans.add(span);
    return span;
  }

  @Override
  public ProcessSpan startProcessSpan(ProcessSpanInfo info) {
    if (throwOnStart) {
      throw new IllegalStateException("tracer exploded on startProcessSpan");
    }
    RecordedProcessSpan span = new RecordedProcessSpan(info);
    processSpans.add(span);
    return span;
  }

  private static Map<String, String> cannedContext(int seq) {
    return Map.of("traceparent", String.format("00-%032x-%016x-01", seq, seq));
  }

  public static final class RecordedPublishSpan implements PublishSpan {

    public final UUID eventId;
    public final String eventType;
    public final Map<String, String> context;
    public volatile UUID coalescedInto;
    public volatile Throwable error;
    public final AtomicInteger closeCount = new AtomicInteger();

    private RecordedPublishSpan(UUID eventId, String eventType, Map<String, String> context) {
      this.eventId = eventId;
      this.eventType = eventType;
      this.context = context;
    }

    @Override
    public Map<String, String> contextToStore() {
      return context;
    }

    @Override
    public void coalesced(UUID existingEventId) {
      this.coalescedInto = existingEventId;
    }

    @Override
    public void error(Throwable error) {
      this.error = error;
    }

    @Override
    public void close() {
      closeCount.incrementAndGet();
    }
  }

  public static final class RecordedProcessSpan implements ProcessSpan {

    public final ProcessSpanInfo info;
    public volatile Throwable error;
    public final AtomicInteger closeCount = new AtomicInteger();

    private RecordedProcessSpan(ProcessSpanInfo info) {
      this.info = info;
    }

    @Override
    public void error(Throwable error) {
      this.error = error;
    }

    @Override
    public void close() {
      closeCount.incrementAndGet();
    }
  }
}
