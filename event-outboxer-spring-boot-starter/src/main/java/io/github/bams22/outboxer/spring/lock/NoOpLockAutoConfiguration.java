/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.lock;

import io.github.bams22.outboxer.spi.EntityLocker;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * Default lock configuration — {@link EntityLocker#NOOP}. Kicks in when no handler declares
 * {@code extractLockKey} and the user has not configured a specific backend.
 */
@AutoConfiguration
@ConditionalOnProperty(
    prefix = "event-outboxer.lock",
    name = "type",
    havingValue = "noop",
    matchIfMissing = true)
public class NoOpLockAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(EntityLocker.class)
  public EntityLocker outboxEntityLocker() {
    return EntityLocker.NOOP;
  }
}
