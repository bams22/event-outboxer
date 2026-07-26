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
 * Spring Boot auto-configuration for event-outboxer. Binds {@code event-outboxer.*} YAML
 * properties,
 * wires Spring-aware collaborators ({@code TransactionAwareDataSourceProxy},
 * {@code ContextPropagatingTaskDecorator}), registers an {@code OutboxEngine} as a
 * {@code SmartLifecycle} bean, and optionally publishes metrics and a health indicator.
 */
@NullMarked
package io.github.bams22.outboxer.spring;

import org.jspecify.annotations.NullMarked;
