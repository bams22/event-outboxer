/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.api.observer;

import io.github.bams22.outboxer.domain.WorkerId;
import java.time.Instant;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Payload of {@link OutboxListener#onEngineCrashed(EngineCrashedInfo)} — fired when the engine's
 * background health check determines that a critical component is no longer alive (for example, a
 * per-type poller thread died from an uncaught {@code Error}). After this callback the engine
 * reports {@code state() == STOPPED}; {@code /actuator/health/outbox} and every metric / listener
 * signal flip to the DOWN / stopped branch.
 *
 * <p>Listeners must treat this event as a fatal condition for the current worker: the engine does
 * not attempt to self-recover. In a Spring Boot deployment with {@code
 * event-outboxer.health.probe-groups: [readiness, liveness]} Kubernetes will drain and restart the
 * pod automatically.
 *
 * @param reason human-readable description of what the health check detected
 * @param cause the underlying throwable that killed the component, if known. Usually {@code null} —
 *     the poller loop swallows {@code RuntimeException}; only unrecoverable {@code Error}s exit the
 *     loop, and those are typically uncaught by our code.
 * @param at wall-clock time the crash was detected
 * @param workerId identifier of the worker JVM
 */
public record EngineCrashedInfo(
    String reason, @Nullable Throwable cause, Instant at, WorkerId workerId) {

  public EngineCrashedInfo {
    Objects.requireNonNull(reason, "reason must not be null");
    if (reason.isBlank()) {
      throw new IllegalArgumentException("reason must not be blank");
    }
    Objects.requireNonNull(at, "at must not be null");
    Objects.requireNonNull(workerId, "workerId must not be null");
  }
}
