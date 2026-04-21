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
 * Reusable abstract JUnit 5 contract tests that every SPI adapter must satisfy. Packaged as a
 * test-jar of {@code event-outboxer-spi} so that adapter modules (storage-inmemory,
 * storage-postgres, lock-postgres, lock-redis) can extend them in their own test sources and
 * inherit the full behavioural specification for free.
 */
package io.github.bams22.outboxer.spi.contracts;
