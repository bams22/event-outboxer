/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.admin.actuator;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.endpoint.OperationType;
import org.springframework.boot.actuate.endpoint.invoke.OperationParameter;
import org.springframework.boot.actuate.endpoint.invoke.reflect.OperationMethod;

/**
 * Regression guard for parameter optionality as seen by Actuator's reflective invoker.
 *
 * <p>{@code OperationMethodParameter.isMandatory()} only recognizes {@code
 * org.springframework.lang.Nullable} (and JSR-305) — the JSpecify {@code @Nullable} alone is
 * invisible to it, which silently turns every optional query parameter into a mandatory one and
 * fails requests with HTTP 400. These tests go through the same {@link OperationMethod} model the
 * real invoker uses, so they fail if the Spring annotation is ever dropped.
 */
class OutboxAdminEndpointParameterOptionalityTest {

    @Test
    @DisplayName("events(): all four query parameters are optional")
    void eventsParametersAreOptional() throws Exception {
        Map<String, Boolean> mandatory =
                mandatoryByName("events", String.class, String.class, Integer.class, String.class);

        assertThat(mandatory)
                .containsOnlyKeys("status", "eventType", "limit", "cursor")
                .allSatisfy((name, isMandatory) -> assertThat(isMandatory).as(name).isFalse());
    }

    @Test
    @DisplayName("reenableAll(): eventType is mandatory, limit is optional")
    void reenableAllParameterOptionality() throws Exception {
        Map<String, Boolean> mandatory =
                mandatoryByName("reenableAll", String.class, Integer.class);

        assertThat(mandatory).containsEntry("eventType", true).containsEntry("limit", false);
    }

    @Test
    @DisplayName(
            "purge(): target and olderThanDays are mandatory, eventType and limit are optional")
    void purgeParameterOptionality() throws Exception {
        Map<String, Boolean> mandatory =
                mandatoryByName("purge", String.class, long.class, String.class, Integer.class);

        assertThat(mandatory)
                .containsEntry("target", true)
                .containsEntry("olderThanDays", true)
                .containsEntry("eventType", false)
                .containsEntry("limit", false);
    }

    private static Map<String, Boolean> mandatoryByName(String methodName, Class<?>... paramTypes)
            throws NoSuchMethodException {
        Method method = OutboxAdminEndpoint.class.getMethod(methodName, paramTypes);
        OperationMethod operationMethod = new OperationMethod(method, OperationType.READ);
        Map<String, Boolean> byName = new LinkedHashMap<>();
        for (OperationParameter parameter : operationMethod.getParameters()) {
            byName.put(parameter.getName(), parameter.isMandatory());
        }
        return byName;
    }
}
