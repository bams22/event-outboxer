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
 * Default {@link io.github.bams22.outboxer.api.track.OutboxEventTracker} (ADR-0038): a stateless
 * reader over the {@code EventStore} and, optionally, the {@code OutboxAdmin} archive lookup.
 *
 * <p>{@link org.jspecify.annotations.NullMarked}: everything is non-null by default.
 */
@NullMarked
package io.github.bams22.outboxer.core.track;

import org.jspecify.annotations.NullMarked;
