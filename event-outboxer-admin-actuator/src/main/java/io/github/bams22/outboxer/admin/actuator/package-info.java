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
 * Spring Boot Actuator surface over the OutboxAdmin SPI port: inspect and re-enable DISABLED
 * events, purge the archive and old failures (ADR-0019).
 */
@NullMarked
package io.github.bams22.outboxer.admin.actuator;

import org.jspecify.annotations.NullMarked;
