/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.storage.postgres.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FlatMapJsonTest {

    @Test
    void emptyRoundTrip() {
        assertThat(FlatMapJson.serialize(Map.of())).isEqualTo("{}");
        assertThat(FlatMapJson.parse("{}")).isEmpty();
    }

    @Test
    void singleEntryRoundTrip() {
        Map<String, String> in = Map.of("traceparent", "00-abc-xyz-01");
        String json = FlatMapJson.serialize(in);
        assertThat(json).isEqualTo("{\"traceparent\":\"00-abc-xyz-01\"}");
        assertThat(FlatMapJson.parse(json)).isEqualTo(in);
    }

    @Test
    void escapesSpecialCharacters() {
        Map<String, String> in = new LinkedHashMap<>();
        in.put("k", "a\"b\\c\nd");
        String json = FlatMapJson.serialize(in);
        assertThat(json).isEqualTo("{\"k\":\"a\\\"b\\\\c\\nd\"}");
        assertThat(FlatMapJson.parse(json)).isEqualTo(in);
    }

    @Test
    void rejectsNonObjectInput() {
        assertThatThrownBy(() -> FlatMapJson.parse("[1, 2]"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNonStringValues() {
        assertThatThrownBy(() -> FlatMapJson.parse("{\"k\":42}"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void parsesNullLiteralAsEmpty() {
        assertThat(FlatMapJson.parse("null")).isEmpty();
    }
}
