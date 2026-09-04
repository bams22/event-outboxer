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

/** Handler executor flavour, bound to {@code event-outboxer.handler-executor.type}. */
public enum ExecutorType {
    PLATFORM("platform"),
    VIRTUAL("virtual");

    private final String property;

    ExecutorType(String property) {
        this.property = property;
    }

    /** The value the starter property expects. */
    public String property() {
        return property;
    }

    /** Parses the command-line spelling (the starter property value, case-insensitive). */
    public static ExecutorType parse(String value) {
        for (ExecutorType type : values()) {
            if (type.property.equalsIgnoreCase(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException(
                "Unknown executor type '" + value + "', expected platform or virtual");
    }
}
