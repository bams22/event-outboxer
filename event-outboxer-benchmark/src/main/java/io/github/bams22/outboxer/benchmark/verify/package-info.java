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
 * Grading: the invariant checker that turns a ledger plus the published set into a pass/fail
 * verdict (ADR-0034 §5), and the chaos events that explain the duplicates a kill or an outage is
 * allowed to produce. Pure functions, unit-tested without a database.
 */
@NullMarked
package io.github.bams22.outboxer.benchmark.verify;

import org.jspecify.annotations.NullMarked;
