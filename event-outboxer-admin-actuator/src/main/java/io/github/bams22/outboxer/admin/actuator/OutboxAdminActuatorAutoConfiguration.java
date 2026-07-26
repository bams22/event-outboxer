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

import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.OutboxAdmin;
import org.springframework.boot.actuate.autoconfigure.endpoint.condition.ConditionalOnAvailableEndpoint;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Registers {@link OutboxAdminEndpoint} when an {@link OutboxAdmin} bean is present (wired by the
 * starter's storage auto-configuration) and the endpoint is available per the standard Actuator
 * exposure rules.
 */
@AutoConfiguration
@ConditionalOnBean({OutboxAdmin.class, EventStore.class})
public class OutboxAdminActuatorAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnAvailableEndpoint(endpoint = OutboxAdminEndpoint.class)
  public OutboxAdminEndpoint outboxAdminEndpoint(OutboxAdmin admin, EventStore store) {
    return new OutboxAdminEndpoint(admin, store);
  }
}
