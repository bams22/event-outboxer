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
 * Publish-side contracts: {@link io.github.bams22.outboxer.api.publish.OutboxEventPublisher}
 * and its companion records {@link io.github.bams22.outboxer.api.publish.PublishOptions} and
 * {@link io.github.bams22.outboxer.api.publish.PublishRequest}.
 *
 * <p>{@link org.jspecify.annotations.NullMarked}: everything is non-null by default.
 */
@NullMarked
package io.github.bams22.outboxer.api.publish;

import org.jspecify.annotations.NullMarked;
