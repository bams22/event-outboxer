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
 * Qualifier for the {@link javax.sql.DataSource} the outbox should use in applications that define
 * more than one (ADR-0024). Mirrors Spring Boot's own subsystem qualifiers
 * ({@code @FlywayDataSource}, {@code @BatchDataSource}, {@code @QuartzDataSource}).
 *
 * <p>The starter resolves the outbox {@code DataSource} in this order:
 *
 * <ol>
 *   <li>the single bean marked with {@code @OutboxDataSource} — wins even over an unrelated
 *       {@code @Primary} bean;
 *   <li>otherwise the unique {@code DataSource} bean, or the {@code @Primary} one among several;
 *   <li>otherwise startup fails fast with the candidate bean names and the fix.
 * </ol>
 *
 * <p>One qualifier governs all outbox JDBC — the PostgreSQL storage adapter and both PostgreSQL
 * entity lockers (their tables live in the same database by design). Example:
 *
 * <pre>{@code
 * @Bean
 * @OutboxDataSource
 * DataSource ordersDataSource() {
 *   return DataSourceBuilder.create()...build();
 * }
 * }</pre>
 *
 * <p>Note: only Java-config beans can carry the qualifier. A {@code DataSource} derived purely from
 * {@code spring.datasource.*} properties participates via the primary/unique rule instead.
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
public @interface OutboxDataSource {}
