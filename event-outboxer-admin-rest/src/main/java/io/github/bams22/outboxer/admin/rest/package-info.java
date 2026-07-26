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
 * Opt-in admin REST surface over the OutboxAdmin SPI port, guarded by a configurable authority
 * via method security (ADR-0019).
 */
@org.jspecify.annotations.NullMarked
package io.github.bams22.outboxer.admin.rest;
