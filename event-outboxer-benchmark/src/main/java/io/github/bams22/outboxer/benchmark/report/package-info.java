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
 * The result of a run: latency statistics, the report record with its environment block, and the
 * writer that produces the JSON file and the console summary (ADR-0034 §6-§7).
 */
@NullMarked
package io.github.bams22.outboxer.benchmark.report;

import org.jspecify.annotations.NullMarked;
