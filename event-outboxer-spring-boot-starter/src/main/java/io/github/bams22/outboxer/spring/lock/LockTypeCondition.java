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

import io.github.bams22.outboxer.spring.OutboxProperties;
import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Matches when {@code event-outboxer.lock.type} binds to the expected {@link
 * OutboxProperties.LockType}.
 *
 * <p>Deliberately NOT a raw {@code @ConditionalOnProperty(havingValue = ...)}: that annotation
 * compares the raw property string, while the enum binds through relaxed binding — for a hyphenated
 * value like {@code postgres-lease} a user writing {@code postgres_lease} (or {@code
 * POSTGRES-LEASE}) would bind to the enum just fine yet silently match no lock autoconfiguration,
 * failing the context with a cryptic missing-{@code EntityLocker} error (ADR-0022 §Starter
 * integration). Binding here keeps the condition and the bound property in lockstep for every
 * accepted spelling.
 */
abstract class LockTypeCondition extends SpringBootCondition {

  private final OutboxProperties.LockType expected;

  LockTypeCondition(OutboxProperties.LockType expected) {
    this.expected = expected;
  }

  @Override
  public final ConditionOutcome getMatchOutcome(
      ConditionContext context, AnnotatedTypeMetadata metadata) {
    OutboxProperties.LockType type =
        Binder.get(context.getEnvironment())
            .bind("event-outboxer.lock.type", OutboxProperties.LockType.class)
            .orElse(OutboxProperties.LockType.noop);
    if (type == expected) {
      return ConditionOutcome.match("event-outboxer.lock.type=" + type);
    }
    return ConditionOutcome.noMatch("event-outboxer.lock.type is " + type + ", not " + expected);
  }
}
