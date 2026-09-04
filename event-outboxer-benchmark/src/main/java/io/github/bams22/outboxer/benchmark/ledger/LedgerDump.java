/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.ledger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

/**
 * Writes a ledger snapshot as CSV, sorted by sequence number then start time, so a failed run can
 * be investigated from the work directory: which worker handled a duplicated event, when, on which
 * attempt, and how far from the chaos event.
 */
public final class LedgerDump {

    private LedgerDump() {}

    /** Writes {@code handlings} to {@code file} and returns it. */
    public static Path write(Path file, List<Handling> handlings) {
        List<Handling> sorted =
                handlings.stream()
                        .sorted(
                                Comparator.comparingLong(Handling::seq)
                                        .thenComparing(Handling::startedAt))
                        .toList();
        try {
            Files.createDirectories(file.getParent());
            try (BufferedWriter out = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
                out.write(
                        "seq,eventType,attempt,workerId,thread,lockKey,startedAt,finishedAt,outcome\n");
                for (Handling h : sorted) {
                    out.write(
                            h.seq()
                                    + ","
                                    + h.eventType()
                                    + ","
                                    + h.attempt()
                                    + ","
                                    + h.workerId()
                                    + ","
                                    + h.thread()
                                    + ","
                                    + (h.lockKey() == null ? "" : h.lockKey())
                                    + ","
                                    + h.startedAt()
                                    + ","
                                    + h.finishedAt()
                                    + ","
                                    + h.outcome()
                                    + "\n");
                }
            }
            return file;
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot write the ledger dump " + file, e);
        }
    }
}
