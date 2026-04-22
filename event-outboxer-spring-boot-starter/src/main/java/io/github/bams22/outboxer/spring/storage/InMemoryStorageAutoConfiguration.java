/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.storage;

import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.ConnectionSupplier;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.WorkerRegistry;
import io.github.bams22.outboxer.storage.inmemory.InMemoryConnectionSupplier;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryWorkerRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * Registers the in-memory {@code EventStore} / {@code WorkerRegistry} when {@code
 * outbox.storage.type=inmemory} (the default). Suitable for tests and local development where
 * durability is not required.
 */
@AutoConfiguration
@ConditionalOnClass(InMemoryEventStore.class)
@ConditionalOnProperty(
    prefix = "event-outboxer.storage",
    name = "type",
    havingValue = "inmemory",
    matchIfMissing = true)
public class InMemoryStorageAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(EventStore.class)
  public InMemoryEventStore outboxEventStore(Clock clock) {
    return new InMemoryEventStore(clock);
  }

  @Bean
  @ConditionalOnMissingBean(WorkerRegistry.class)
  public InMemoryWorkerRegistry outboxWorkerRegistry(Clock clock) {
    return new InMemoryWorkerRegistry(clock);
  }

  @Bean
  @ConditionalOnMissingBean(ConnectionSupplier.class)
  public InMemoryConnectionSupplier outboxConnectionSupplier() {
    return new InMemoryConnectionSupplier();
  }
}
