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
 * Handler-side contracts: {@link io.github.bams22.outboxer.api.handle.EventHandler} plus the sealed
 * {@link io.github.bams22.outboxer.api.handle.EventOutcome} and {@link
 * io.github.bams22.outboxer.api.handle.FailureDecision}, together with their supporting contexts
 * and the {@link io.github.bams22.outboxer.api.handle.FailureHandler} chain-of- responsibility
 * interface.
 *
 * <p>{@link org.jspecify.annotations.NullMarked}: everything is non-null by default.
 */
@NullMarked
package io.github.bams22.outboxer.api.handle;

import org.jspecify.annotations.NullMarked;
