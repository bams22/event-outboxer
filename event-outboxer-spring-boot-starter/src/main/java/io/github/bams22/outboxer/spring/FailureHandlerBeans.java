/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring;

import io.github.bams22.outboxer.api.handle.FailureHandler;
import io.github.bams22.outboxer.domain.exception.InvariantViolationException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * The {@link FailureHandler} beans the engine registers (ADR-0030), collected from the bean
 * factory:
 *
 * <ul>
 *   <li>{@code @OutboxFailureHandler} without a value → the global chain;
 *   <li>{@code @OutboxFailureHandler({"A", "B"})} → the chain for each listed type;
 *   <li>legacy: a bean named (or {@code @Qualifier}-ed) {@code outboxDefaultFailureHandler} →
 *       global; a {@code Map<String, FailureHandler<?>>} bean named {@code
 *       outboxPerTypeFailureHandlers} → one claim per entry;
 *   <li>any other {@code FailureHandler} bean → {@link #unregistered()} — reported in a startup
 *       warning, never silently ignored.
 * </ul>
 *
 * <p>Each slot (global, or one event type) accepts exactly one claim; a second distinct source
 * fails startup with {@link AmbiguousOutboxFailureHandlerException}.
 *
 * @param global the global chain bean, or {@code null}
 * @param globalSource bean name of {@code global}, or {@code null}
 * @param perType chains keyed by event type
 * @param perTypeSources bean name (or legacy map entry) behind each {@code perType} key
 * @param unregistered names of {@code FailureHandler} beans that claim no slot
 */
record FailureHandlerBeans(
        @Nullable FailureHandler<?> global,
        @Nullable String globalSource,
        Map<String, FailureHandler<?>> perType,
        Map<String, String> perTypeSources,
        List<String> unregistered) {

    static final String LEGACY_DEFAULT = "outboxDefaultFailureHandler";
    static final String LEGACY_PER_TYPE = "outboxPerTypeFailureHandlers";

    FailureHandlerBeans {
        perType = Map.copyOf(perType);
        perTypeSources = Map.copyOf(perTypeSources);
        unregistered = List.copyOf(unregistered);
    }

    /** Scans the bean factory; instantiates only the beans that claim a slot. */
    static FailureHandlerBeans collect(ListableBeanFactory beanFactory) {
        List<String> globalClaims = new ArrayList<>();
        Map<String, List<String>> perTypeClaims = new LinkedHashMap<>();
        List<String> unregistered = new ArrayList<>();

        for (String name : beanFactory.getBeanNamesForType(FailureHandler.class, true, false)) {
            OutboxFailureHandler marker =
                    beanFactory.findAnnotationOnBean(name, OutboxFailureHandler.class);
            if (marker != null) {
                if (marker.value().length == 0) {
                    globalClaims.add(name);
                } else {
                    for (String type : marker.value()) {
                        if (type.isBlank()) {
                            throw new InvariantViolationException(
                                    "@OutboxFailureHandler on bean '"
                                            + name
                                            + "' lists a blank event type");
                        }
                        perTypeClaims.computeIfAbsent(type, t -> new ArrayList<>()).add(name);
                    }
                }
            } else if (isLegacyDefault(beanFactory, name)) {
                globalClaims.add(name);
            } else {
                unregistered.add(name);
            }
        }

        Map<String, FailureHandler<?>> legacyPerType = legacyPerTypeMap(beanFactory);
        for (String type : legacyPerType.keySet()) {
            perTypeClaims
                    .computeIfAbsent(type, t -> new ArrayList<>())
                    .add(LEGACY_PER_TYPE + "[" + type + "]");
        }

        if (globalClaims.size() > 1) {
            throw AmbiguousOutboxFailureHandlerException.multipleGlobal(globalClaims);
        }
        for (Map.Entry<String, List<String>> claim : perTypeClaims.entrySet()) {
            if (claim.getValue().size() > 1) {
                throw AmbiguousOutboxFailureHandlerException.multiplePerType(
                        claim.getKey(), claim.getValue());
            }
        }

        FailureHandler<?> global = null;
        String globalSource = null;
        if (!globalClaims.isEmpty()) {
            globalSource = globalClaims.get(0);
            global = beanFactory.getBean(globalSource, FailureHandler.class);
        }
        Map<String, FailureHandler<?>> perType = new LinkedHashMap<>();
        Map<String, String> perTypeSources = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> claim : perTypeClaims.entrySet()) {
            String type = claim.getKey();
            String source = claim.getValue().get(0);
            FailureHandler<?> chain =
                    source.startsWith(LEGACY_PER_TYPE + "[")
                            ? legacyPerType.get(type)
                            : beanFactory.getBean(source, FailureHandler.class);
            perType.put(type, chain);
            perTypeSources.put(type, source);
        }
        return new FailureHandlerBeans(global, globalSource, perType, perTypeSources, unregistered);
    }

    /**
     * Text of the startup warning for {@link #unregistered()} beans, or empty when there are none.
     */
    Optional<String> unregisteredWarning() {
        if (unregistered.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(
                "FailureHandler beans "
                        + unregistered
                        + " are neither annotated with @OutboxFailureHandler nor named "
                        + LEGACY_DEFAULT
                        + " / "
                        + LEGACY_PER_TYPE
                        + ", so the outbox engine does not use them. Annotate them (no value ="
                        + " global chain, value = event types), or ignore this warning if the bean"
                        + " is returned from an EventHandler.failureHandler() override.");
    }

    private static boolean isLegacyDefault(ListableBeanFactory beanFactory, String name) {
        if (LEGACY_DEFAULT.equals(name)) {
            return true;
        }
        Qualifier qualifier = beanFactory.findAnnotationOnBean(name, Qualifier.class);
        return qualifier != null && LEGACY_DEFAULT.equals(qualifier.value());
    }

    private static Map<String, FailureHandler<?>> legacyPerTypeMap(
            ListableBeanFactory beanFactory) {
        if (!beanFactory.containsBean(LEGACY_PER_TYPE)) {
            return Map.of();
        }
        Object bean = beanFactory.getBean(LEGACY_PER_TYPE);
        if (!(bean instanceof Map<?, ?> map)) {
            throw new InvariantViolationException(
                    "bean '"
                            + LEGACY_PER_TYPE
                            + "' must be a Map<String, FailureHandler<?>>, got "
                            + bean.getClass().getName());
        }
        Map<String, FailureHandler<?>> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> e : map.entrySet()) {
            if (!(e.getKey() instanceof String type)
                    || !(e.getValue() instanceof FailureHandler<?> chain)) {
                throw new InvariantViolationException(
                        "bean '"
                                + LEGACY_PER_TYPE
                                + "' must map event types to FailureHandler instances; entry "
                                + e.getKey()
                                + " -> "
                                + (e.getValue() == null
                                        ? "null"
                                        : e.getValue().getClass().getName())
                                + " does not");
            }
            result.put(type, chain);
        }
        return result;
    }
}
