/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * Service Provider Interfaces consumed by the core engine and implemented by adapter modules.
 *
 * <p>This package defines the ports that the engine calls into: {@link
 * io.github.bams22.outboxer.spi.EventStore} for persistence, {@link
 * io.github.bams22.outboxer.spi.WorkerRegistry} for heartbeat management, {@link
 * io.github.bams22.outboxer.spi.EntityLocker} for business-key locking, {@link
 * io.github.bams22.outboxer.spi.EventSerializer} for payload serialization, {@link
 * io.github.bams22.outboxer.spi.Clock} for time abstraction, and {@link
 * io.github.bams22.outboxer.spi.ConnectionSupplier} so that the PostgreSQL storage adapter can
 * participate in the caller's transaction without depending on Spring (see ADR-0002).
 *
 * <p>Everything in this package is {@code @NullMarked} per ADR-0018: method parameters, return
 * types and record components are non-null unless explicitly annotated with {@link
 * org.jspecify.annotations.Nullable}.
 */
@NullMarked
package io.github.bams22.outboxer.spi;

import org.jspecify.annotations.NullMarked;
