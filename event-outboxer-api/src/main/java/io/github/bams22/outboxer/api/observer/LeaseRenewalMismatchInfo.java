/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.api.observer;

/**
 * Payload of {@link OutboxListener#onLeaseRenewalMismatch(LeaseRenewalMismatchInfo)} — fired
 * when the number of rows updated by a lease-renewal sweep did not match the number requested,
 * indicating either that some events finalized between sweeps or that another actor altered the
 * rows.
 *
 * @param requestedCount number of events the sweep tried to refresh
 * @param actualCount number of events the storage actually updated
 */
public record LeaseRenewalMismatchInfo(int requestedCount, int actualCount) {

  public LeaseRenewalMismatchInfo {
    if (requestedCount < 0) {
      throw new IllegalArgumentException("requestedCount must be >= 0, got " + requestedCount);
    }
    if (actualCount < 0) {
      throw new IllegalArgumentException("actualCount must be >= 0, got " + actualCount);
    }
  }
}
