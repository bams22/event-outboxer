/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.domain;

/**
 * Lifecycle status of an outbox event.
 *
 * <p>Terminal for the <em>active</em> lifecycle: a successful event is deleted from the active
 * store (optionally moved to an archive table; see ADR-0008). DISABLED events are retained in the
 * active store for administrative inspection and can be reactivated through an admin API.
 */
public enum EventStatus {

  /**
   * Event is waiting to be processed. It becomes eligible for {@code claim} once {@code runAt <=
   * now}. A new event created through {@code OutboxEventPublisher.publish(...)} starts in this
   * status, as does an event returned from PROCESSING through retry, orphan recovery, or forced
   * reclaim.
   */
  PENDING,

  /**
   * Event has been claimed by a worker and is currently being processed. Every PROCESSING row has
   * a non-null {@code claimedBy} and {@code claimedAt} (enforced by a CHECK constraint in the
   * PostgreSQL adapter).
   */
  PROCESSING,

  /**
   * Event will not be processed automatically. Either the retry policy exhausted its attempts or
   * an {@code EventHandler} explicitly returned {@code EventOutcome.Fail}. The row stays in place
   * for investigation and manual re-activation.
   */
  DISABLED
}
