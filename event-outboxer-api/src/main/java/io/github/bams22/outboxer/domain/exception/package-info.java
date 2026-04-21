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
 * {@link io.github.bams22.outboxer.domain.exception.OutboxException} root and the six category
 * bases with their concrete finals (Publish-, Handle-, Storage-, Lock-, Configuration-,
 * EngineLifecycle-). Operational exceptions carry an {@code OUTBOX-XXX} message code.
 *
 * <p>{@link org.jspecify.annotations.NullMarked}: everything is non-null by default.
 */
@NullMarked
package io.github.bams22.outboxer.domain.exception;

import org.jspecify.annotations.NullMarked;
