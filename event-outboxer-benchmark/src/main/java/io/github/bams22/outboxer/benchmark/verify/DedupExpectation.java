/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.verify;

import java.util.Map;
import java.util.Objects;

/**
 * What a run with dedup keys promises, per key: the moment the key's <em>last</em> publish had
 * committed (microseconds since the epoch, taken by the driver right after the publish call
 * returned). The checker demands a successful handling of that key that started after it — the
 * ADR-0021 visibility guarantee: the effect of every coalesced publish is seen by a handler, so in
 * particular the final state of each key is. A publish that coalesced is never handled under its
 * own sequence number, so the per-sequence "lost" rule does not apply to such a run.
 *
 * @param lastCommitMicrosByKey per dedup key, the driver-side commit instant of its last publish
 */
public record DedupExpectation(Map<String, Long> lastCommitMicrosByKey) {

    public DedupExpectation {
        lastCommitMicrosByKey =
                Map.copyOf(
                        Objects.requireNonNull(
                                lastCommitMicrosByKey, "lastCommitMicrosByKey must not be null"));
    }
}
