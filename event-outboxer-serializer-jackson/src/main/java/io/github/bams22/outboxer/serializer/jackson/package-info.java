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
 * Jackson implementation of {@code EventSerializer} plus a factory for the default outbox {@code
 * ObjectMapper}. The adapter holds no state and is safe for concurrent use.
 */
@NullMarked
package io.github.bams22.outboxer.serializer.jackson;

import org.jspecify.annotations.NullMarked;
