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

/**
 * Wake-up signal from the publish side to the polling side of the SAME JVM. After a transaction
 * that inserted an event of type {@code T} commits, the publisher calls {@code wake(T)} so the
 * poller for {@code T} claims immediately instead of sleeping out the remainder of its adaptive
 * poll interval — dropping same-JVM publish→handle latency from seconds to milliseconds (ADR-0006
 * amendment).
 *
 * <p>Purely an optimization: a missed or spurious wake is always safe, polling remains the
 * correctness mechanism. Events published by other JVMs are still picked up poll-bound.
 */
@FunctionalInterface
public interface PollerWaker {

  /**
   * Wake the local poller responsible for {@code eventType}, if one exists. Unknown types are a
   * no-op. Never throws.
   */
  void wake(String eventType);

  /** No-op waker for wirings without local pollers (testkit's ManualEngine, bare publishers). */
  PollerWaker NOOP = eventType -> {};
}
