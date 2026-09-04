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
 * The handling ledger: the harness's own record of every handler invocation, independent of the
 * library's listeners and metrics (ADR-0034 §4).
 */
@NullMarked
package io.github.bams22.outboxer.benchmark.ledger;

import org.jspecify.annotations.NullMarked;
