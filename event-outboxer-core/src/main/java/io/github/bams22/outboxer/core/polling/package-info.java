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
 * Per-event-type poller. One poller thread loops {@code PollStrategy.pollOnce()} for a given
 * event type, handing claimed events to the {@code HandlerDispatcher}.
 */
@NullMarked
package io.github.bams22.outboxer.core.polling;

import org.jspecify.annotations.NullMarked;
