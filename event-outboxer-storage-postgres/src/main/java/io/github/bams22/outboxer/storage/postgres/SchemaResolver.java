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

import java.util.Objects;

/**
 * Resolves fully-qualified table names given the configured {@link
 * PostgresStorageProperties#schema()} and {@link PostgresStorageProperties#tablePrefix()}. Used by
 * the event-store and worker-registry adapters to build SQL statements at construction time — the
 * results are cached so the hot path does not do string concatenation per claim.
 */
public final class SchemaResolver {

  private final String eventsTable;
  private final String workersTable;
  private final String archiveTable;

  public SchemaResolver(PostgresStorageProperties props) {
    Objects.requireNonNull(props, "props must not be null");
    String prefix = props.tablePrefix();
    this.eventsTable = props.schema() + "." + prefix + "events";
    this.workersTable = props.schema() + "." + prefix + "workers";
    this.archiveTable = props.schema() + "." + prefix + "event_archive";
  }

  public String events() {
    return eventsTable;
  }

  public String workers() {
    return workersTable;
  }

  public String archive() {
    return archiveTable;
  }
}
