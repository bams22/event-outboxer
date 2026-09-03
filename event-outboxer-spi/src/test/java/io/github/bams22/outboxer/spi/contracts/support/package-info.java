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
 * Test fixtures shared through the {@code event-outboxer-spi} test-jar: a deliberately non-JSON
 * binary {@code EventSerializer} and its payload DTO, used to prove that engine and storage
 * adapters carry the binary payload lane verbatim (ADR-0025), and an inert {@code OutboxAdmin} stub
 * for tests that need the port but assert on only a slice of it.
 */
@NullMarked
package io.github.bams22.outboxer.spi.contracts.support;

import org.jspecify.annotations.NullMarked;
