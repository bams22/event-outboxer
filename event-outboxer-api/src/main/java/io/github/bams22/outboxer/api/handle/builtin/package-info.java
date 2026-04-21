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
 * Built-in {@link io.github.bams22.outboxer.api.handle.FailureHandler} implementations —
 * three leaves (no-retry / fixed-delay / exponential-backoff) and three decorators
 * (log / notify-listener / max-retries) — plus a fluent {@link
 * io.github.bams22.outboxer.api.handle.builtin.FailureHandlerBuilder} and the {@link
 * io.github.bams22.outboxer.api.handle.builtin.FailureHandlers#defaults()} factory.
 *
 * <p>{@link org.jspecify.annotations.NullMarked}: everything is non-null by default.
 */
@NullMarked
package io.github.bams22.outboxer.api.handle.builtin;

import org.jspecify.annotations.NullMarked;
