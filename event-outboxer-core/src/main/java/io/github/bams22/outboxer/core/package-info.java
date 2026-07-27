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
 * Spring-agnostic engine of event-outboxer. Contains the poller, handler dispatcher, in-flight
 * registry, maintenance tasks, default publisher and the {@code OutboxEngine} facade that composes
 * them against whatever SPI adapters are configured.
 *
 * <p>This module depends only on {@code event-outboxer-api}, {@code event-outboxer-spi} and SLF4J.
 * The {@code maven-enforcer-plugin} bans {@code org.springframework.*} and {@code
 * jakarta.persistence.*} on the core classpath.
 */
@NullMarked
package io.github.bams22.outboxer.core;

import org.jspecify.annotations.NullMarked;
