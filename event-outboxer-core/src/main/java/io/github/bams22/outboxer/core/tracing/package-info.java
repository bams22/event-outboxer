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
 * Engine-internal tracing support: defensive wrapping of the {@code OutboxTracer} SPI port
 * (ADR-0023) so that a misbehaving tracing adapter can never break publish or dispatch.
 */
@NullMarked
package io.github.bams22.outboxer.core.tracing;

import org.jspecify.annotations.NullMarked;
