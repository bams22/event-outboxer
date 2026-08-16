/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.metrics;

import java.io.IOException;
import java.util.List;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;

/**
 * Applies the Micrometer distribution defaults shipped with the {@code
 * event-outboxer-metrics-micrometer} module ({@code META-INF/event-outboxer/metrics-defaults.yml}):
 * SLO histogram buckets for the event-outboxer timers, so fleet-wide {@code histogram_quantile()}
 * works in Prometheus without any per-application configuration.
 *
 * <p>The property source is appended with {@code addLast} — the weakest precedence — so any {@code
 * management.metrics.distribution.*} value set by the application (config files, environment
 * variables, a config server) always overrides these defaults. Opt out entirely with {@code
 * event-outboxer.metrics.distribution-defaults.enabled=false}.
 *
 * <p>Does nothing when the metrics module (and therefore the YAML resource) is not on the
 * classpath.
 *
 * <p>Registered via {@code META-INF/spring.factories}; runs at {@link Ordered#LOWEST_PRECEDENCE} so
 * config files (including profile-specific and imported ones) are loaded first and the opt-out
 * property is honoured wherever it is declared.
 */
public class OutboxMetricsDefaultsEnvironmentPostProcessor
        implements EnvironmentPostProcessor, Ordered {

    /** Property source we contribute under — named for easy identification in debug dumps. */
    static final String PROPERTY_SOURCE_NAME = "event-outboxer-metrics-defaults";

    static final String ENABLED_PROPERTY = "event-outboxer.metrics.distribution-defaults.enabled";

    static final String RESOURCE = "META-INF/event-outboxer/metrics-defaults.yml";

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment env, SpringApplication application) {
        if (!env.getProperty(ENABLED_PROPERTY, Boolean.class, true)) {
            return;
        }
        ClassPathResource resource = new ClassPathResource(RESOURCE);
        if (!resource.exists()) {
            // event-outboxer-metrics-micrometer is not on the classpath — nothing to default.
            return;
        }
        try {
            List<PropertySource<?>> loaded =
                    new YamlPropertySourceLoader().load(PROPERTY_SOURCE_NAME, resource);
            for (PropertySource<?> propertySource : loaded) {
                env.getPropertySources().addLast(propertySource);
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to load " + RESOURCE, ex);
        }
    }
}
