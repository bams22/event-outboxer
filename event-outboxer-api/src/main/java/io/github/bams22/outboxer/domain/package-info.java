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
 * Immutable value objects shared across the public API and the SPI. All types in this package
 * are {@link org.jspecify.annotations.NullMarked NullMarked}: references are non-null unless
 * explicitly annotated with {@link org.jspecify.annotations.Nullable Nullable}.
 */
@NullMarked
package io.github.bams22.outboxer.domain;

import org.jspecify.annotations.NullMarked;
