/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.config;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class MaintenanceConfigTest {

    @Test
    void rejectsDeadThresholdSmallerThanThreeHeartbeats() {
        assertThatThrownBy(
                        () ->
                                MaintenanceConfig.builder()
                                        .heartbeatInterval(Duration.ofSeconds(5))
                                        .deadThreshold(Duration.ofSeconds(10))
                                        .orphanRecoveryInterval(Duration.ofSeconds(10))
                                        .watchdogInterval(Duration.ofSeconds(10))
                                        .abandonedHandlerGrace(Duration.ofSeconds(1))
                                        .reclaimBatchSize(10)
                                        .shutdownTimeout(Duration.ofSeconds(10))
                                        .staleClaimSweepInterval(Duration.ofMinutes(5))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("deadThreshold must be >= 3 * heartbeatInterval");
    }
}
