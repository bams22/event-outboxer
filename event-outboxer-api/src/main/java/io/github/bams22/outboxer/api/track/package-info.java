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
 * Tracking an event by its id (ADR-0038): {@link
 * io.github.bams22.outboxer.api.track.OutboxEventTracker} looks up what an observer can know about
 * an event — {@link io.github.bams22.outboxer.api.track.TrackedState} — and waits, bounded, for it
 * to be finalised — {@link io.github.bams22.outboxer.api.track.AwaitResult}. The id returned by
 * {@code OutboxEventPublisher.publish(...)} is the handle; no handler result value is ever stored
 * or returned.
 *
 * <p>{@link org.jspecify.annotations.NullMarked}: everything is non-null by default.
 */
@NullMarked
package io.github.bams22.outboxer.api.track;

import org.jspecify.annotations.NullMarked;
