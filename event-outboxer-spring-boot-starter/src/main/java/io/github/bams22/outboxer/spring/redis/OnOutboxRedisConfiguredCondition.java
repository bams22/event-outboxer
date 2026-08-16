/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.redis;

import io.github.bams22.outboxer.spring.OutboxProperties;
import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.util.StringUtils;

/**
 * Matches when the application configured a starter-managed Redis connection: {@code
 * event-outboxer.redis.uri} or {@code event-outboxer.redis.host} is set (ADR-0027).
 *
 * <p>Not a raw {@code @ConditionalOnProperty} because that annotation cannot express "either of two
 * properties"; binding the whole {@code event-outboxer.redis} group keeps the condition in lockstep
 * with what {@code OutboxLettuceConnectionManager} will actually see (same rationale as {@code
 * LockTypeCondition}).
 */
class OnOutboxRedisConfiguredCondition extends SpringBootCondition {

    @Override
    public ConditionOutcome getMatchOutcome(
            ConditionContext context, AnnotatedTypeMetadata metadata) {
        OutboxProperties.Redis redis =
                Binder.get(context.getEnvironment())
                        .bind("event-outboxer.redis", OutboxProperties.Redis.class)
                        .orElseGet(OutboxProperties.Redis::new);
        if (StringUtils.hasText(redis.getUri())) {
            return ConditionOutcome.match("event-outboxer.redis.uri is set");
        }
        if (StringUtils.hasText(redis.getHost())) {
            return ConditionOutcome.match("event-outboxer.redis.host is set");
        }
        return ConditionOutcome.noMatch(
                "neither event-outboxer.redis.uri nor event-outboxer.redis.host is set");
    }
}
