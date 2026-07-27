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
 * Auto-configuration of the {@code OutboxTracer} SPI port (ADR-0023): detects Micrometer Tracing
 * or the OpenTelemetry API on the classpath and wires the matching adapter into the publisher and
 * the engine.
 */
@org.jspecify.annotations.NullMarked
package io.github.bams22.outboxer.spring.tracing;
