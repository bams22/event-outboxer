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
 * Starter-managed Lettuce connection (ADR-0027): property-driven {@code StatefulRedisConnection}
 * creation and the {@code @OutboxRedisConnection} resolution machinery shared by the Redis entity
 * locker and the Redis metrics cache auto-configurations.
 */
@NullMarked
package io.github.bams22.outboxer.spring.redis;

import org.jspecify.annotations.NullMarked;
