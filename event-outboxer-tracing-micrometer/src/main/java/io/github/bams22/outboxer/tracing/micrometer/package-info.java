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
 * Micrometer Tracing adapter for the {@code OutboxTracer} SPI port (ADR-0023): continues the
 * publisher's distributed trace into handler execution via the event row's {@code trace_context}
 * carrier, honouring Spring Boot's propagation and baggage configuration.
 */
@org.jspecify.annotations.NullMarked
package io.github.bams22.outboxer.tracing.micrometer;
