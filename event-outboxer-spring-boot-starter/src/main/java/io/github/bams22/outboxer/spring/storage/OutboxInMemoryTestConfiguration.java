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
import io.github.bams22.outboxer.spi.OutboxAdmin;
import io.github.bams22.outboxer.spi.WorkerRegistry;
import io.github.bams22.outboxer.storage.inmemory.InMemoryConnectionSupplier;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryOutboxAdmin;
import io.github.bams22.outboxer.storage.inmemory.InMemoryWorkerRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * NON-DURABLE in-memory outbox wiring for TESTS. Deliberately a plain {@code @Configuration}, never
 * an auto-configuration (ADR-0020): events stored here do not participate in the caller's
 * transaction and do not survive a restart, so in-memory storage must be an unmistakably explicit
 * choice, unreachable through {@code event-outboxer.*} properties. Use it in Spring tests only:
 *
 * <pre>
 * &#64;SpringBootTest
 * &#64;Import(OutboxInMemoryTestConfiguration.class)
 * class MyOutboxTest { ... }
 * </pre>
 *
 * <p>Requires {@code event-outboxer-storage-inmemory} on the (test) classpath.
 */
@Configuration(proxyBeanMethods = false)
public class OutboxInMemoryTestConfiguration {

  @Bean
  @ConditionalOnMissingBean(EventStore.class)
  public InMemoryEventStore outboxEventStore(ObjectProvider<Clock> clock) {
    return new InMemoryEventStore(clock.getIfAvailable(Clock::system));
  }

  @Bean
  @ConditionalOnMissingBean(WorkerRegistry.class)
  public InMemoryWorkerRegistry outboxWorkerRegistry(ObjectProvider<Clock> clock) {
    return new InMemoryWorkerRegistry(clock.getIfAvailable(Clock::system));
  }

  @Bean
  @ConditionalOnMissingBean(ConnectionSupplier.class)
  public InMemoryConnectionSupplier outboxConnectionSupplier() {
    return new InMemoryConnectionSupplier();
  }

  @Bean
  @ConditionalOnMissingBean(OutboxAdmin.class)
  @ConditionalOnBean(InMemoryEventStore.class)
  public InMemoryOutboxAdmin outboxAdmin(InMemoryEventStore store) {
    return new InMemoryOutboxAdmin(store);
  }
}
