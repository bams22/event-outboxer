/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.OutboxTracer;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class SafeOutboxTracerTest {

  private static final OutboxTracer.ProcessSpanInfo INFO =
      new OutboxTracer.ProcessSpanInfo(UUID.randomUUID(), "T", 1, new WorkerId("w-1"), Map.of());

  /** Tracer whose every method throws. */
  private static final OutboxTracer EXPLODING =
      new OutboxTracer() {
        @Override
        public PublishSpan startPublishSpan(UUID eventId, String eventType) {
          throw new IllegalStateException("startPublishSpan exploded");
        }

        @Override
        public ProcessSpan startProcessSpan(ProcessSpanInfo info) {
          throw new IllegalStateException("startProcessSpan exploded");
        }
      };

  /** Tracer whose spans start fine but explode on every handle method. */
  private static final OutboxTracer EXPLODING_HANDLES =
      new OutboxTracer() {
        @Override
        public PublishSpan startPublishSpan(UUID eventId, String eventType) {
          return new PublishSpan() {
            @Override
            public Map<String, String> contextToStore() {
              throw new IllegalStateException("contextToStore exploded");
            }

            @Override
            public void coalesced(UUID existingEventId) {
              throw new IllegalStateException("coalesced exploded");
            }

            @Override
            public void error(Throwable error) {
              throw new IllegalStateException("error exploded");
            }

            @Override
            public void close() {
              throw new IllegalStateException("close exploded");
            }
          };
        }

        @Override
        public ProcessSpan startProcessSpan(ProcessSpanInfo info) {
          return new ProcessSpan() {
            @Override
            public void error(Throwable error) {
              throw new IllegalStateException("error exploded");
            }

            @Override
            public void close() {
              throw new IllegalStateException("close exploded");
            }
          };
        }
      };

  @Test
  void wrapIsIdentityForNoopAndAlreadyWrapped() {
    assertThat(SafeOutboxTracer.wrap(OutboxTracer.NOOP)).isSameAs(OutboxTracer.NOOP);

    OutboxTracer wrapped = SafeOutboxTracer.wrap(EXPLODING);
    assertThat(SafeOutboxTracer.wrap(wrapped)).isSameAs(wrapped);
  }

  @Test
  void throwingSpanStartDegradesToNoop() {
    OutboxTracer safe = SafeOutboxTracer.wrap(EXPLODING);

    OutboxTracer.PublishSpan publish = safe.startPublishSpan(UUID.randomUUID(), "T");
    assertThat(publish.contextToStore()).isEmpty();

    assertThatCode(
            () -> {
              publish.error(new RuntimeException("x"));
              publish.close();
              OutboxTracer.ProcessSpan process = safe.startProcessSpan(INFO);
              process.error(new RuntimeException("x"));
              process.close();
            })
        .doesNotThrowAnyException();
  }

  @Test
  void throwingHandleMethodsAreSwallowed() {
    OutboxTracer safe = SafeOutboxTracer.wrap(EXPLODING_HANDLES);

    OutboxTracer.PublishSpan publish = safe.startPublishSpan(UUID.randomUUID(), "T");
    assertThat(publish.contextToStore()).isEmpty(); // fallback on failure

    assertThatCode(
            () -> {
              publish.coalesced(UUID.randomUUID());
              publish.error(new RuntimeException("x"));
              publish.close();
              OutboxTracer.ProcessSpan process = safe.startProcessSpan(INFO);
              process.error(new RuntimeException("x"));
              process.close();
            })
        .doesNotThrowAnyException();
  }

  @Test
  void delegatesToWorkingTracer() {
    io.github.bams22.outboxer.core.support.RecordingOutboxTracer recording =
        new io.github.bams22.outboxer.core.support.RecordingOutboxTracer();
    OutboxTracer safe = SafeOutboxTracer.wrap(recording);

    UUID id = UUID.randomUUID();
    OutboxTracer.PublishSpan span = safe.startPublishSpan(id, "T");
    Map<String, String> context = span.contextToStore();
    span.close();

    assertThat(recording.publishSpans).hasSize(1);
    assertThat(recording.publishSpans.get(0).eventId).isEqualTo(id);
    assertThat(context).containsKey("traceparent");
    assertThat(recording.publishSpans.get(0).closeCount).hasValue(1);
  }
}
