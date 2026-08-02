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

/**
 * Matches when {@code event-outboxer.lock.type} binds to {@link
 * OutboxProperties.LockType#postgres_advisory}. See {@link LockTypeCondition} for why this is a
 * Binder-based condition rather than a raw string match.
 */
final class OnPostgresAdvisoryLockCondition extends LockTypeCondition {

    OnPostgresAdvisoryLockCondition() {
        super(OutboxProperties.LockType.postgres_advisory);
    }
}
