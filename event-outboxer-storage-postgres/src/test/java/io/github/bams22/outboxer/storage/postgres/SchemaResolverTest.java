/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.storage.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class SchemaResolverTest {

    @Test
    void defaultSchemaAndNoPrefix() {
        SchemaResolver r = new SchemaResolver(PostgresStorageProperties.defaults());
        assertThat(r.events()).isEqualTo("event_outboxer.events");
        assertThat(r.workers()).isEqualTo("event_outboxer.workers");
        assertThat(r.archive()).isEqualTo("event_outboxer.event_archive");
    }

    @Test
    void appliesPrefix() {
        PostgresStorageProperties props =
                PostgresStorageProperties.builder()
                        .schema("myapp")
                        .tablePrefix("ob_")
                        .archiveEnabled(true)
                        .metricsCacheTtl(Duration.ofSeconds(30))
                        .build();
        SchemaResolver r = new SchemaResolver(props);
        assertThat(r.events()).isEqualTo("myapp.ob_events");
        assertThat(r.workers()).isEqualTo("myapp.ob_workers");
        assertThat(r.archive()).isEqualTo("myapp.ob_event_archive");
    }
}
