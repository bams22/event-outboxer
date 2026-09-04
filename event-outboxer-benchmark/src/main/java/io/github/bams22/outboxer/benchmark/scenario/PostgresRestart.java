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

/**
 * How PostgreSQL is taken down under the fleet. Only the disposable Testcontainers database can be
 * restarted; the signal goes to the container's postmaster.
 */
public enum PostgresRestart {
    /** No restart. */
    NONE("none", ""),
    /**
     * {@code SIGINT} = fast shutdown: open transactions abort, the server exits cleanly, no crash
     * recovery on start. What {@code pg_ctl restart -m fast} and a planned failover do.
     */
    FAST("fast", "SIGINT"),
    /**
     * {@code SIGKILL} = the postmaster dies without writing a shutdown checkpoint; the next start
     * replays WAL. What an OOM-kill or a node loss looks like.
     */
    CRASH("crash", "SIGKILL");

    private final String option;
    private final String signal;

    PostgresRestart(String option, String signal) {
        this.option = option;
        this.signal = signal;
    }

    /** The command-line spelling. */
    public String option() {
        return option;
    }

    /** The signal sent to the container's main process; empty for {@link #NONE}. */
    public String signal() {
        return signal;
    }

    /** Parses the command-line spelling, case-insensitive. */
    public static PostgresRestart parse(String value) {
        for (PostgresRestart mode : values()) {
            if (mode.option.equalsIgnoreCase(value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException(
                "Unknown PostgreSQL restart mode '" + value + "', expected none, fast or crash");
    }
}
