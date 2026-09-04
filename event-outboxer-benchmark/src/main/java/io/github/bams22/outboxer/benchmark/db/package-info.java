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
 * The benchmark database: coordinates, the disposable-vs-external handle, and the {@code pg_stat}
 * probe that prices a run in row writes (ADR-0034 §6).
 */
@NullMarked
package io.github.bams22.outboxer.benchmark.db;

import org.jspecify.annotations.NullMarked;
