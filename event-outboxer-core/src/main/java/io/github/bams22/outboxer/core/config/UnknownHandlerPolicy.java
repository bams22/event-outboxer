/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.config;

/**
 * What the dispatcher does when a claimed event has no registered {@code EventHandler} for its
 * type (see ADR-0013). Expected in typical multi-service deployments where one instance rolls out
 * a new handler ahead of a peer.
 */
public enum UnknownHandlerPolicy {

  /**
   * Re-schedule the event with a short delay so that a peer instance — or a future deploy of this
   * one — can pick it up later. Default.
   */
  SKIP,

  /** Move the event to {@code DISABLED} immediately; an operator has to re-enable it. */
  DISABLE,

  /**
   * Leave the event {@code PROCESSING} and raise an exception so the row falls back to orphan
   * recovery. Reserved for strict environments that would rather fail loudly than route the
   * event elsewhere.
   */
  FAIL
}
