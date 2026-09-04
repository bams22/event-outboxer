/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.target;

/**
 * The publish side of a target. One call must leave the event durably stored under the target's own
 * transaction semantics — for event-outboxer that is one business transaction around {@code
 * OutboxEventPublisher.publish} (ADR-0002). The driver times the whole call and invokes it from
 * several threads concurrently.
 */
@FunctionalInterface
public interface BenchmarkPublisher {

    /** Publishes one event and returns once it is committed. */
    void publish(BenchmarkEvent event);
}
