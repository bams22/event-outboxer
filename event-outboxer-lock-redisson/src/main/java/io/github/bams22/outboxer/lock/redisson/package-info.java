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
 * {@code EntityLocker} over a Redisson {@code RLock} (ADR-0036): the application's existing {@code
 * RedissonClient}, Redisson's own pub/sub wait for the bounded lock wait of ADR-0035, and every
 * topology Redisson supports.
 */
@NullMarked
package io.github.bams22.outboxer.lock.redisson;

import org.jspecify.annotations.NullMarked;
