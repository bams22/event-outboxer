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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

class OutboxProbeGroupsEnvironmentPostProcessorTest {

  private final OutboxProbeGroupsEnvironmentPostProcessor epp =
      new OutboxProbeGroupsEnvironmentPostProcessor();
  private final SpringApplication app = new SpringApplication();

  @Test
  void noOp_whenPropertyUnset() {
    StandardEnvironment env = new StandardEnvironment();

    epp.postProcessEnvironment(env, app);

    assertThat(env.getPropertySources().contains("event-outboxer-probe-groups")).isFalse();
  }

  @Test
  void seedsDefaultStateIndicator_whenUserHasNoIncludes() {
    StandardEnvironment env = envWith("outbox.health.probe-groups", "readiness");

    epp.postProcessEnvironment(env, app);

    String include = env.getProperty("management.endpoint.health.group.readiness.include");
    assertThat(include).isEqualTo("readinessState,outbox");
  }

  @Test
  void mergesWithUserIncludes_preservingOrderAndUniqueness() {
    StandardEnvironment env =
        envWith(
            Map.of(
                "outbox.health.probe-groups", "readiness",
                "management.endpoint.health.group.readiness.include", "readinessState, db "));

    epp.postProcessEnvironment(env, app);

    String include = env.getProperty("management.endpoint.health.group.readiness.include");
    assertThat(include).isEqualTo("readinessState,db,outbox");
  }

  @Test
  void supportsMultipleGroups() {
    StandardEnvironment env = envWith("outbox.health.probe-groups", "readiness,liveness");

    epp.postProcessEnvironment(env, app);

    assertThat(env.getProperty("management.endpoint.health.group.readiness.include"))
        .isEqualTo("readinessState,outbox");
    assertThat(env.getProperty("management.endpoint.health.group.liveness.include"))
        .isEqualTo("livenessState,outbox");
  }

  @Test
  void doesNotDuplicateOutboxIfUserAlreadyIncludedIt() {
    StandardEnvironment env =
        envWith(
            Map.of(
                "outbox.health.probe-groups", "readiness",
                "management.endpoint.health.group.readiness.include",
                    "readinessState,outbox,custom"));

    epp.postProcessEnvironment(env, app);

    String include = env.getProperty("management.endpoint.health.group.readiness.include");
    assertThat(include).isEqualTo("readinessState,outbox,custom");
  }

  @Test
  void runsAtLowestPrecedence() {
    assertThat(epp.getOrder()).isEqualTo(Integer.MAX_VALUE);
  }

  // ---------------------------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------------------------

  private static StandardEnvironment envWith(String key, String value) {
    return envWith(Map.of(key, value));
  }

  private static StandardEnvironment envWith(Map<String, String> properties) {
    StandardEnvironment env = new StandardEnvironment();
    env.getPropertySources()
        .addFirst(new MapPropertySource("test", new HashMap<>(properties)));
    return env;
  }
}
