/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.scenario;

/** How the worker fleet is hosted (ADR-0034 §3). */
public enum FleetMode {
    /**
     * Phase 1: one Spring context per worker inside the driver JVM. Cheap, shares CPU and GC with
     * the publisher, cannot be crashed.
     */
    IN_PROCESS("in-process"),
    /**
     * Phase 2: one JVM per worker forked from the driver. Separate heaps, honest {@code SIGKILL},
     * ledger in the database.
     */
    FORKED("forked");

    private final String option;

    FleetMode(String option) {
        this.option = option;
    }

    /** The command-line spelling. */
    public String option() {
        return option;
    }

    /** Parses the command-line spelling, case-insensitive. */
    public static FleetMode parse(String value) {
        for (FleetMode mode : values()) {
            if (mode.option.equalsIgnoreCase(value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException(
                "Unknown fleet mode '" + value + "', expected in-process or forked");
    }
}
