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
 * Observability event bus — {@link io.github.bams22.outboxer.api.observer.OutboxListener} and
 * its 21 {@code *Info} record types. Implementations typically publish metrics (see the
 * {@code event-outboxer-metrics-micrometer} module), emit structured logs, or maintain an
 * audit trail.
 *
 * <p>{@link org.jspecify.annotations.NullMarked}: everything is non-null by default.
 */
@NullMarked
package io.github.bams22.outboxer.api.observer;

import org.jspecify.annotations.NullMarked;
