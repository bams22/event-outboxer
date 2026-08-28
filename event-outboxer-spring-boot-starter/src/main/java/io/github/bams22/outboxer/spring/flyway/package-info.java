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
 * Starter-managed Flyway instance that applies the outbox schema migrations independently of the
 * application's {@code spring.flyway.*} instance (ADR-0028).
 */
@NullMarked
package io.github.bams22.outboxer.spring.flyway;

import org.jspecify.annotations.NullMarked;
