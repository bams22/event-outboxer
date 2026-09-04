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
 * The event-outboxer target: the library booted as deployed — Spring Boot starter, PostgreSQL
 * storage, starter-managed Flyway — as one publish-only context plus a fleet of workers, either
 * Spring contexts in this JVM or forked JVMs (ADR-0034 §3).
 */
@NullMarked
package io.github.bams22.outboxer.benchmark.target.outboxer;

import org.jspecify.annotations.NullMarked;
