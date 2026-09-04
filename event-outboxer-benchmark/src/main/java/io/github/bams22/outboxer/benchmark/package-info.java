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
 * The benchmark and invariant harness (ADR-0034): {@code BenchmarkRunner} is the entry point,
 * {@code BenchmarkOptions} turns {@code --bench.*} arguments into a scenario, {@code BenchmarkRun}
 * drives one run end to end. Never published; see the module documentation for how to run it.
 */
@NullMarked
package io.github.bams22.outboxer.benchmark;

import org.jspecify.annotations.NullMarked;
