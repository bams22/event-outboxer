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
 * Which serializer writes the benchmark payload, bound to {@code
 * event-outboxer.serializer.write-format} (ADR-0025). Both formats carry the same three fields
 * ({@code seq}, {@code lockKey}, {@code padding}); Jackson lands in the JSONB text lane, Protobuf
 * in the BYTEA binary lane.
 */
public enum PayloadFormat {
    JACKSON("jackson-json"),
    PROTOBUF("protobuf");

    private final String property;

    PayloadFormat(String property) {
        this.property = property;
    }

    /** The serializer's format id, as the starter property expects it. */
    public String property() {
        return property;
    }

    /** Parses the command-line spelling: the format id or the short name, case-insensitive. */
    public static PayloadFormat parse(String value) {
        for (PayloadFormat format : values()) {
            if (format.property.equalsIgnoreCase(value) || format.name().equalsIgnoreCase(value)) {
                return format;
            }
        }
        throw new IllegalArgumentException(
                "Unknown payload format '" + value + "', expected jackson or protobuf");
    }
}
