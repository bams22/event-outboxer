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
 * In-memory SPI adapter intended for unit tests and non-durable development setups. Provides
 * thread-safe implementations of {@code EventStore}, {@code WorkerRegistry} and {@code
 * EntityLocker}, plus a stub {@code ConnectionSupplier} that fails fast (no JDBC in the in-memory
 * adapter).
 */
@NullMarked
package io.github.bams22.outboxer.storage.inmemory;

import org.jspecify.annotations.NullMarked;
