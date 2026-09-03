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
 * Keyset cursor over the archive for {@link OutboxAdmin#replayAllFromArchive}: the {@code
 * (archivedAt, id)} of the last archive row the previous batch <em>considered</em> — replayed,
 * coalesced or skipped alike (ADR-0033).
 *
 * <p>Separate from {@link AdminCursor} because the sort key is different: {@code AdminCursor} pages
 * the hot table by {@code (created_at, id)} descending, this one walks the archive by {@code
 * (archived_at, id)} ascending. The pair, not the instant alone, is what makes the walk exact —
 * archive rows can share an {@code archived_at}, and a cursor on the timestamp alone would skip
 * whichever tied row the previous batch's {@code LIMIT} cut off.
 *
 * @param archivedAt {@code archived_at} of the last row considered
 * @param id id of the last row considered, breaking ties on {@code archivedAt}
 */
public record ArchiveCursor(Instant archivedAt, UUID id) {

    public ArchiveCursor {
        Objects.requireNonNull(archivedAt, "archivedAt must not be null");
        Objects.requireNonNull(id, "id must not be null");
    }
}
