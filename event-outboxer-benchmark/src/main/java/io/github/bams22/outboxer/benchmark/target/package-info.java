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
 * The system-under-test seam (ADR-0034 §2): the driver talks to a target only through {@code
 * BenchmarkTarget}, {@code TargetSession} and {@code BenchmarkPublisher}, so a second outbox
 * implementation can be measured with the same scenarios by writing one adapter elsewhere.
 */
@NullMarked
package io.github.bams22.outboxer.benchmark.target;

import org.jspecify.annotations.NullMarked;
