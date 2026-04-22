/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spi;

import java.util.Objects;
import java.util.Optional;

/**
 * No-op {@link MetricsSnapshotCache}. Exposed as the {@link MetricsSnapshotCache#noop()}
 * singleton. Every {@code get()} misses; every {@code put(...)} is dropped.
 */
final class NoopMetricsSnapshotCache implements MetricsSnapshotCache {

  static final NoopMetricsSnapshotCache INSTANCE = new NoopMetricsSnapshotCache();

  private NoopMetricsSnapshotCache() {}

  @Override
  public Optional<OutboxMetricsSnapshot> get() {
    return Optional.empty();
  }

  @Override
  public void put(OutboxMetricsSnapshot snapshot) {
    Objects.requireNonNull(snapshot, "snapshot must not be null");
  }

  @Override
  public void invalidate() {
    // no-op
  }
}
