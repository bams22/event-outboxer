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

import io.github.bams22.outboxer.lock.redis.RedisEntityLocker;
import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.spring.OutboxProperties;
import io.lettuce.core.api.StatefulRedisConnection;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * Registers {@link RedisEntityLocker} when {@code outbox.lock.type=redis} and a {@link
 * StatefulRedisConnection} bean is available (users create one via Lettuce's {@code RedisClient}).
 */
@AutoConfiguration
@ConditionalOnClass(RedisEntityLocker.class)
@ConditionalOnBean(StatefulRedisConnection.class)
@ConditionalOnProperty(prefix = "event-outboxer.lock", name = "type", havingValue = "redis")
public class RedisLockAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(EntityLocker.class)
  public EntityLocker outboxEntityLocker(
      StatefulRedisConnection<String, String> connection, OutboxProperties properties) {
    return new RedisEntityLocker(connection, properties.getLock().getKeyPrefix());
  }
}
