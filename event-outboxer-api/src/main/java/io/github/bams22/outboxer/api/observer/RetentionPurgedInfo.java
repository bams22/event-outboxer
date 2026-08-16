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
 * Payload of {@link OutboxListener#onRetentionPurged(RetentionPurgedInfo)} — fired when the
 * retention task permanently deleted rows past their retention window. Fires only when at least one
 * row was purged.
 *
 * @param archivedPurged number of rows deleted from the archive table
 * @param disabledPurged number of {@code DISABLED} rows deleted from the events table
 */
public record RetentionPurgedInfo(long archivedPurged, long disabledPurged) {

    public RetentionPurgedInfo {
        if (archivedPurged < 0) {
            throw new IllegalArgumentException("archivedPurged must not be negative");
        }
        if (disabledPurged < 0) {
            throw new IllegalArgumentException("disabledPurged must not be negative");
        }
    }
}
