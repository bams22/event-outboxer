/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.workerid;

import io.github.bams22.outboxer.domain.WorkerId;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Resolves the {@link WorkerId} used by a running engine. By default delegates to {@link
 * WorkerId#generateDefault()} which produces {@code {hostname}-{pid}-{uuid8}}. Callers that need a
 * stable identifier (for example, a Kubernetes StatefulSet pod name) can pass an explicit value to
 * {@link #explicit(WorkerId)}.
 */
public final class WorkerIdFactory {

    private WorkerIdFactory() {}

    /** Default generator — delegates to {@link WorkerId#generateDefault()}. */
    public static Supplier<WorkerId> defaultGenerator() {
        return WorkerId::generateDefault;
    }

    /** Always returns the given id. */
    public static Supplier<WorkerId> explicit(WorkerId id) {
        Objects.requireNonNull(id, "id must not be null");
        return () -> id;
    }
}
