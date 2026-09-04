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
 * Qualifier for the {@code RedissonClient} the outbox's Redisson entity locker should use in
 * applications that define more than one (ADR-0036). Mirrors {@link
 * OutboxRedisConnection @OutboxRedisConnection} (ADR-0027) and {@link
 * OutboxDataSource @OutboxDataSource} (ADR-0024).
 *
 * <p>Resolution order for {@code event-outboxer.lock.type=redisson}:
 *
 * <ol>
 *   <li>the single bean marked with {@code @OutboxRedissonClient} — wins even over an unrelated
 *       {@code @Primary} bean;
 *   <li>otherwise the unique {@code RedissonClient} bean, or the {@code @Primary} one among
 *       several;
 *   <li>otherwise startup fails fast with the candidate bean names and the fix.
 * </ol>
 *
 * <p>The starter never creates a {@code RedissonClient} of its own: the point of the Redisson
 * locker is to ride the client the application already runs (typically from {@code
 * redisson-spring-boot-starter}).
 */
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Qualifier
public @interface OutboxRedissonClient {}
