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
import io.github.bams22.outboxer.api.handle.builtin.FailureHandlerBuilder;
import io.github.bams22.outboxer.api.handle.builtin.FailureHandlers;
import io.github.bams22.outboxer.api.handle.builtin.MaxRetriesFailureHandler.ExhaustedAction;
import io.github.bams22.outboxer.domain.exception.InvariantViolationException;
import io.github.bams22.outboxer.spring.OutboxProperties.FailureStrategy;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;
import org.slf4j.event.Level;
import org.springframework.boot.logging.LogLevel;

/**
 * Builds {@link FailureHandler} chains from {@code event-outboxer.event-types.*.failure.*}
 * (ADR-0030) with the same thin merge the other per-type knobs use: a per-type override sets only
 * the keys it declares, everything else comes from {@code defaults.failure.*}, and unset defaults
 * fall back to the library chain {@code FailureHandlers.defaults()}.
 *
 * <p>Validation happens per layer, so an error names the exact property the user wrote — {@code
 * event-outboxer.event-types.overrides.SEND_EMAIL.failure.multiplier must be > 1.0, got 1.0} — and
 * the cross-field rule ({@code max-delay >= base-delay}) is checked on the merged policy.
 */
final class FailurePolicyFactory {

    static final String DEFAULTS_PATH = "event-outboxer.event-types.defaults.failure";

    private FailurePolicyFactory() {}

    /** Property path of a per-type override, for error messages. */
    static String overridePath(String eventType) {
        return "event-outboxer.event-types.overrides." + eventType + ".failure";
    }

    /** Fully resolved policy — the failure-chain analogue of {@code EventTypeConfig}. */
    record FailurePolicy(
            FailureStrategy strategy,
            int maxAttempts,
            ExhaustedAction exhaustedAction,
            Duration baseDelay,
            double multiplier,
            Duration maxDelay,
            double jitter,
            Duration fixedDelay,
            LogLevel logLevel) {

        /**
         * Mirrors {@code FailureHandlers.defaults()}: Log(WARN) → MaxRetries(10, DISABLE) →
         * ExponentialBackoff(5s, ×2.0, cap 1h, jitter 0.2); the fixed-delay default (30s) only
         * matters once {@code strategy=fixed} is chosen.
         */
        static final FailurePolicy LIBRARY_DEFAULTS =
                new FailurePolicy(
                        FailureStrategy.exponential,
                        10,
                        ExhaustedAction.DISABLE,
                        Duration.ofSeconds(5),
                        2.0,
                        Duration.ofHours(1),
                        0.2,
                        Duration.ofSeconds(30),
                        LogLevel.WARN);
    }

    /** {@code true} when no {@code failure.*} key is set at this level. */
    static boolean isUnset(OutboxProperties.Failure f) {
        return f.getStrategy() == null
                && f.getMaxAttempts() == null
                && f.getExhaustedAction() == null
                && f.getBaseDelay() == null
                && f.getMultiplier() == null
                && f.getMaxDelay() == null
                && f.getJitter() == null
                && f.getFixedDelay() == null
                && f.getLogLevel() == null;
    }

    /**
     * The global chain from {@code defaults.failure.*}, or {@code null} when nothing is set there —
     * the caller then leaves the engine on {@code FailureHandlers.defaults()}.
     */
    static @Nullable FailureHandler<?> defaultChain(OutboxProperties.EventTypes eventTypes) {
        OutboxProperties.Failure defaults = eventTypes.getDefaults().getFailure();
        if (isUnset(defaults)) {
            return null;
        }
        return toChain(
                merge(defaults, FailurePolicy.LIBRARY_DEFAULTS, DEFAULTS_PATH), DEFAULTS_PATH);
    }

    /**
     * One chain per override whose {@code failure.*} sets anything, layered on the resolved
     * defaults (YAML defaults over library defaults — never on a Java bean, which is opaque).
     */
    static Map<String, FailureHandler<?>> perTypeChains(OutboxProperties.EventTypes eventTypes) {
        FailurePolicy resolvedDefaults =
                merge(
                        eventTypes.getDefaults().getFailure(),
                        FailurePolicy.LIBRARY_DEFAULTS,
                        DEFAULTS_PATH);
        Map<String, FailureHandler<?>> chains = new LinkedHashMap<>();
        for (Map.Entry<String, OutboxProperties.EventType> e :
                eventTypes.getOverrides().entrySet()) {
            OutboxProperties.Failure override = e.getValue().getFailure();
            if (isUnset(override)) {
                continue;
            }
            String path = overridePath(e.getKey());
            chains.put(e.getKey(), toChain(merge(override, resolvedDefaults, path), path));
        }
        return chains;
    }

    /**
     * Field-by-field overlay of one properties layer onto a resolved base; every key set in the
     * layer is validated against its own range so the error names that key.
     */
    static FailurePolicy merge(OutboxProperties.Failure layer, FailurePolicy base, String path) {
        Integer maxAttempts = layer.getMaxAttempts();
        if (maxAttempts != null && maxAttempts < 1) {
            throw violation(path, "max-attempts", "must be >= 1", maxAttempts);
        }
        Duration baseDelay = layer.getBaseDelay();
        if (baseDelay != null && (baseDelay.isZero() || baseDelay.isNegative())) {
            throw violation(path, "base-delay", "must be positive", baseDelay);
        }
        Double multiplier = layer.getMultiplier();
        if (multiplier != null && !(multiplier > 1.0)) {
            throw violation(path, "multiplier", "must be > 1.0", multiplier);
        }
        Duration maxDelay = layer.getMaxDelay();
        if (maxDelay != null && (maxDelay.isZero() || maxDelay.isNegative())) {
            throw violation(path, "max-delay", "must be positive", maxDelay);
        }
        Double jitter = layer.getJitter();
        if (jitter != null && (jitter < 0.0 || jitter > 1.0)) {
            throw violation(path, "jitter", "must be in [0, 1]", jitter);
        }
        Duration fixedDelay = layer.getFixedDelay();
        if (fixedDelay != null && (fixedDelay.isZero() || fixedDelay.isNegative())) {
            throw violation(path, "fixed-delay", "must be positive", fixedDelay);
        }
        return new FailurePolicy(
                orElse(layer.getStrategy(), base.strategy()),
                orElse(maxAttempts, base.maxAttempts()),
                orElse(layer.getExhaustedAction(), base.exhaustedAction()),
                orElse(baseDelay, base.baseDelay()),
                orElse(multiplier, base.multiplier()),
                orElse(maxDelay, base.maxDelay()),
                orElse(jitter, base.jitter()),
                orElse(fixedDelay, base.fixedDelay()),
                orElse(layer.getLogLevel(), base.logLevel()));
    }

    /** Assembles the chain: Log (unless {@code OFF}) → MaxRetries (unless {@code none}) → leaf. */
    static FailureHandler<?> toChain(FailurePolicy policy, String path) {
        if (policy.strategy() == FailureStrategy.exponential
                && policy.maxDelay().compareTo(policy.baseDelay()) < 0) {
            throw new InvariantViolationException(
                    path
                            + ".max-delay ("
                            + policy.maxDelay()
                            + ") must be >= "
                            + path
                            + ".base-delay ("
                            + policy.baseDelay()
                            + ") — effective values after the thin merge");
        }
        FailureHandlerBuilder<Object> builder = FailureHandlers.builder();
        Level level = toSlf4j(policy.logLevel());
        if (level != null) {
            builder.withLogging(level);
        }
        if (policy.strategy() != FailureStrategy.none) {
            builder.withMaxAttempts(policy.maxAttempts(), policy.exhaustedAction());
        }
        try {
            return switch (policy.strategy()) {
                case exponential ->
                        builder.withExponentialBackoff(
                                policy.baseDelay(),
                                policy.multiplier(),
                                policy.maxDelay(),
                                policy.jitter());
                case fixed -> builder.withFixedDelay(policy.fixedDelay());
                case none -> builder.withNoRetry();
            };
        } catch (IllegalArgumentException ex) {
            // Belt and braces: the per-layer checks above should have caught everything.
            throw new InvariantViolationException(path + ": " + ex.getMessage());
        }
    }

    /**
     * Boot's {@link LogLevel} → SLF4J; {@code OFF} means no logging decorator, {@code FATAL} has no
     * SLF4J equivalent and maps to {@code ERROR}.
     */
    static @Nullable Level toSlf4j(LogLevel level) {
        return switch (level) {
            case TRACE -> Level.TRACE;
            case DEBUG -> Level.DEBUG;
            case INFO -> Level.INFO;
            case WARN -> Level.WARN;
            case ERROR, FATAL -> Level.ERROR;
            case OFF -> null;
        };
    }

    private static InvariantViolationException violation(
            String path, String key, String rule, Object actual) {
        return new InvariantViolationException(
                path + "." + key + " " + rule + ", got " + Objects.toString(actual));
    }

    private static <T> T orElse(@Nullable T value, T fallback) {
        return value != null ? value : fallback;
    }
}
