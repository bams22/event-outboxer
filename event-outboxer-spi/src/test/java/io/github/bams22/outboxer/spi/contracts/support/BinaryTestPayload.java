/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spi.contracts.support;

/**
 * Fixed DTO encoded by {@link BinaryTestEventSerializer} — the explicit-DTO rule of ADR-0003
 * applies to test payloads too.
 */
public record BinaryTestPayload(String name, int number) {}
