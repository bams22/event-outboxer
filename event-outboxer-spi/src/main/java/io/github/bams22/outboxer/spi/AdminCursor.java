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

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Keyset-pagination cursor for {@link OutboxAdmin#findByStatus}: the {@code (createdAt, id)} of
 * the last row of the previous page. Keyset instead of offset on purpose — admin queries target
 * exactly the tables where offsets degrade (large DISABLED backlogs, the archive).
 */
public record AdminCursor(Instant createdAt, UUID id) {

  public AdminCursor {
    Objects.requireNonNull(createdAt, "createdAt must not be null");
    Objects.requireNonNull(id, "id must not be null");
  }
}
