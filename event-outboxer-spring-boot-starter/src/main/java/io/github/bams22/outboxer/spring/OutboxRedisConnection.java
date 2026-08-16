/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Qualifier for the Lettuce {@code StatefulRedisConnection<String, String>} the outbox should use
 * in applications that define more than one (ADR-0027). Mirrors {@link
 * OutboxDataSource @OutboxDataSource} (ADR-0024).
 *
 * <p>The starter resolves the outbox Redis connection in this order:
 *
 * <ol>
 *   <li>the single bean marked with {@code @OutboxRedisConnection} — wins even over an unrelated
 *       {@code @Primary} bean;
 *   <li>otherwise the unique {@code StatefulRedisConnection} bean, or the {@code @Primary} one
 *       among several;
 *   <li>otherwise startup fails fast with the candidate bean names and the fix.
 * </ol>
 *
 * <p>One qualifier governs all outbox Redis wiring — the Redis entity locker ({@code
 * event-outboxer.lock.type=redis}) and the Redis metrics cache ({@code
 * event-outboxer.cache.type=redis}) share the connection by design. The connection the starter
 * creates itself from {@code event-outboxer.redis.*} properties carries this qualifier too.
 * Example:
 *
 * <pre>{@code
 * @Bean(destroyMethod = "close")
 * @OutboxRedisConnection
 * StatefulRedisConnection<String, String> outboxRedis(RedisClient client) {
 *   return client.connect();
 * }
 * }</pre>
 */
@Target({
    ElementType.FIELD,
    ElementType.METHOD,
    ElementType.PARAMETER,
    ElementType.TYPE,
    ElementType.ANNOTATION_TYPE
})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Qualifier
public @interface OutboxRedisConnection {}
