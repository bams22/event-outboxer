/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.polling;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Routes {@link PollerWaker#wake(String)} calls to the per-type {@link Poller} instances of this
 * JVM. The engine builder registers every poller it creates; the publisher only ever sees the
 * {@link PollerWaker} interface. In the Spring Boot starter the hub is a shared bean because the
 * {@code OutboxEventPublisher} bean is constructed independently of the engine.
 */
public final class PollerWakeHub implements PollerWaker {

  private static final Logger log = LoggerFactory.getLogger(PollerWakeHub.class);

  private final ConcurrentMap<String, Poller> targets = new ConcurrentHashMap<>();

  /**
   * Register the poller serving its event type. Called by the engine builder; the same hub may
   * be reused across engine restarts because {@link Poller} instances survive stop/start.
   */
  public void register(Poller poller) {
    targets.put(poller.eventType(), poller);
  }

  @Override
  public void wake(String eventType) {
    Poller poller = targets.get(eventType);
    if (poller == null) {
      return;
    }
    try {
      poller.wake();
    } catch (RuntimeException ex) {
      // A wake must never propagate into the publisher's commit path.
      log.debug("poller wake failed for type {}: {}", eventType, ex.toString());
    }
  }
}
