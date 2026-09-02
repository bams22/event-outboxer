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
 * Spring Cloud Stream relay (ADR-0032): {@code StreamOutboxPublisher} stores broker messages in the
 * outbox transactionally, a built-in handler delivers them through {@code StreamBridge}.
 */
@NullMarked
package io.github.bams22.outboxer.relay.stream;

import org.jspecify.annotations.NullMarked;
