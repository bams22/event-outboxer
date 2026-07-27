/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.health;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

/**
 * Appends the {@code outbox} indicator into every Actuator health group listed in {@code
 * outbox.health.probe-groups}. Intended for k8s deployments whose probes hit only {@code
 * /actuator/health/liveness} and {@code /actuator/health/readiness}: opt in and the outbox's {@code
 * UP}/{@code DOWN} state automatically propagates into the probe response.
 *
 * <p>Merges non-destructively: the user's existing {@code
 * management.endpoint.health.group.<name>.include} is preserved, and the default {@code
 * <name>State} contributor (Spring Boot's built-in liveness/readiness probe) is carried through so
 * existing semantics are not lost.
 *
 * <p>Registered via {@code META-INF/spring.factories}; runs at {@link Ordered#LOWEST_PRECEDENCE} so
 * config files (including profile-specific) are loaded first.
 */
public class OutboxProbeGroupsEnvironmentPostProcessor
    implements EnvironmentPostProcessor, Ordered {

  /** Indicator bean name registered by {@code OutboxHealthAutoConfiguration}. */
  static final String INDICATOR_NAME = "outbox";

  /** Property source we contribute under — named for easy identification in debug dumps. */
  static final String PROPERTY_SOURCE_NAME = "event-outboxer-probe-groups";

  static final String PROBE_GROUPS_PROPERTY = "event-outboxer.health.probe-groups";

  @Override
  public int getOrder() {
    return Ordered.LOWEST_PRECEDENCE;
  }

  @Override
  public void postProcessEnvironment(ConfigurableEnvironment env, SpringApplication application) {
    List<String> groups = readProbeGroups(env);
    if (groups.isEmpty()) {
      return;
    }

    Map<String, Object> contributions = new LinkedHashMap<>();
    for (String group : groups) {
      String key = "management.endpoint.health.group." + group + ".include";
      String mergedValue = mergeInclude(env.getProperty(key), group);
      contributions.put(key, mergedValue);
    }

    env.getPropertySources().addFirst(new MapPropertySource(PROPERTY_SOURCE_NAME, contributions));
  }

  /**
   * Reads {@code outbox.health.probe-groups} as a list. Supports both YAML list form and the
   * comma-separated scalar form.
   */
  private static List<String> readProbeGroups(ConfigurableEnvironment env) {
    List<String> fromBinder =
        Binder.get(env)
            .bind(PROBE_GROUPS_PROPERTY, String[].class)
            .map(Arrays::asList)
            .orElse(null);
    if (fromBinder != null && !fromBinder.isEmpty()) {
      return fromBinder.stream().map(String::trim).filter(s -> !s.isEmpty()).toList();
    }
    String raw = env.getProperty(PROBE_GROUPS_PROPERTY);
    if (raw == null || raw.isBlank()) {
      return List.of();
    }
    List<String> out = new ArrayList<>();
    for (String part : raw.split(",")) {
      String trimmed = part.trim();
      if (!trimmed.isEmpty()) {
        out.add(trimmed);
      }
    }
    return out;
  }

  /**
   * Produces the final comma-separated {@code include} value for a group:
   *
   * <ul>
   *   <li>If the user set the key, append {@code outbox} to their list (unless it is already
   *       present).
   *   <li>If the user did not set the key, seed with Spring Boot's default {@code <group>State}
   *       contributor so the stock liveness/readiness wiring still works, then append {@code
   *       outbox}.
   * </ul>
   */
  private static String mergeInclude(@Nullable String existing, String group) {
    Set<String> merged = new LinkedHashSet<>();
    if (existing == null || existing.isBlank()) {
      merged.add(group + "State");
    } else {
      for (String part : existing.split(",")) {
        String trimmed = part.trim();
        if (!trimmed.isEmpty()) {
          merged.add(trimmed);
        }
      }
    }
    merged.add(INDICATOR_NAME);
    return String.join(",", merged);
  }
}
