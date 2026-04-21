/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * PostgreSQL implementation of the storage SPI ports. Spring-free — the adapter only needs a
 * {@code javax.sql.DataSource}-backed {@link
 * io.github.bams22.outboxer.spi.ConnectionSupplier} to participate in the caller's transaction
 * (see ADR-0002).
 */
@NullMarked
package io.github.bams22.outboxer.storage.postgres;

import org.jspecify.annotations.NullMarked;
