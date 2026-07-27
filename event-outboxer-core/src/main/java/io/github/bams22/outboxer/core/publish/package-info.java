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
 * Default {@code OutboxEventPublisher} implementation plus the {@code TransactionContext} port used
 * to enforce the no-transaction policy.
 */
@NullMarked
package io.github.bams22.outboxer.core.publish;

import org.jspecify.annotations.NullMarked;
