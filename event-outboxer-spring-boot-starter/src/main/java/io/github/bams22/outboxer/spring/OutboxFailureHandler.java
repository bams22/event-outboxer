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
 * Registers a {@link io.github.bams22.outboxer.api.handle.FailureHandler} bean with the outbox
 * engine (ADR-0030). Without a value the bean is the <em>global</em> failure chain; with one or
 * more event types it is the chain for exactly those types:
 *
 * <pre>{@code
 * @Bean
 * @OutboxFailureHandler
 * FailureHandler<Object> outboxFailures() {
 *   return FailureHandlers.builder()
 *       .withMaxAttempts(5, ExhaustedAction.DISABLE)
 *       .withExponentialBackoff(Duration.ofSeconds(30), 2.0, Duration.ofHours(2), 0.2);
 * }
 *
 * @Bean
 * @OutboxFailureHandler({"SEND_EMAIL", "SEND_SMS"})
 * FailureHandler<Object> notificationFailures() {
 *   return FailureHandlers.builder().withFixedDelay(Duration.ofMinutes(1));
 * }
 * }</pre>
 *
 * <p>Precedence — the most specific source wins, and Java beats YAML at equal specificity:
 *
 * <ol>
 *   <li>{@code EventHandler.failureHandler()} on the handler itself;
 *   <li>a per-type bean ({@code @OutboxFailureHandler("TYPE")});
 *   <li>{@code event-outboxer.event-types.overrides.TYPE.failure.*};
 *   <li>the global bean ({@code @OutboxFailureHandler} without a value);
 *   <li>{@code event-outboxer.event-types.defaults.failure.*};
 *   <li>the library chain {@code FailureHandlers.defaults()}.
 * </ol>
 *
 * <p>Exactly one bean may claim a slot: two global beans, or two beans naming the same event type,
 * fail startup ({@link AmbiguousOutboxFailureHandlerException}). The pre-ADR-0030 forms — a bean
 * named {@code outboxDefaultFailureHandler} and a {@code Map<String, FailureHandler<?>>} bean named
 * {@code outboxPerTypeFailureHandlers} — keep working and count as claims on the same slots. {@code
 * FailureHandler} beans that carry neither the annotation nor a legacy name are not registered and
 * are listed in a startup warning.
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
public @interface OutboxFailureHandler {

    /** Event types this chain applies to; empty (the default) makes the bean the global chain. */
    String[] value() default {};
}
