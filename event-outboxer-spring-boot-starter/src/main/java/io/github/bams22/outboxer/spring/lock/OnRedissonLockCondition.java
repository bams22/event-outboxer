/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.lock;

import io.github.bams22.outboxer.spring.OutboxProperties;

/** Matches when {@code event-outboxer.lock.type} binds to {@code redisson}. */
final class OnRedissonLockCondition extends LockTypeCondition {

    OnRedissonLockCondition() {
        super(OutboxProperties.LockType.redisson);
    }
}
