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
 * Test support for applications using event-outboxer. Contains {@code SettableClock} for
 * deterministic time-travel, {@code ManualEngine} for single-threaded step-through dispatching,
 * {@code OutboxTestContext} that wires everything against the in-memory adapter, fluent
 * {@code EventAssertions}, a capturing {@code RecordingOutboxListener} and a JUnit 5
 * {@code OutboxExtension} that injects a fresh context per test.
 */
@NullMarked
package io.github.bams22.outboxer.testkit;

import org.jspecify.annotations.NullMarked;
