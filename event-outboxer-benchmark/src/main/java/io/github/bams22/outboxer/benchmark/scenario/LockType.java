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
 * Entity locker under test, bound to {@code event-outboxer.lock.type}. Redis is deliberately absent
 * from the first cut: the harness runs on PostgreSQL alone.
 */
public enum LockType {
    NOOP("noop"),
    POSTGRES_LEASE("postgres-lease"),
    POSTGRES_ADVISORY("postgres-advisory");

    private final String property;

    LockType(String property) {
        this.property = property;
    }

    /** The value the starter property expects. */
    public String property() {
        return property;
    }

    /**
     * Whether this locker actually serialises handlings that share a lock key — the flag that
     * decides if the lock-exclusivity invariant is graded or merely reported.
     */
    public boolean exclusive() {
        return this != NOOP;
    }

    /** Parses the command-line spelling (the starter property value, case-insensitive). */
    public static LockType parse(String value) {
        for (LockType type : values()) {
            if (type.property.equalsIgnoreCase(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException(
                "Unknown lock type '"
                        + value
                        + "', expected noop, postgres-lease or postgres-advisory");
    }
}
