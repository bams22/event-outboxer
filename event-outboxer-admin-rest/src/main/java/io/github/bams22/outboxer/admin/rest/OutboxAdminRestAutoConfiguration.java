/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.admin.rest;

import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.OutboxAdmin;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.util.ClassUtils;

/**
 * Registers the admin REST controller when {@code event-outboxer.admin.rest.enabled=true} and an
 * {@link OutboxAdmin} bean is present.
 *
 * <p>Security posture (ADR-0019): the controller is guarded by {@code @PreAuthorize}, which is
 * enforced by Spring <em>method security</em> — a separate switch from having Spring Security on
 * the classpath. To prevent the silent failure mode "security configured, annotation ignored",
 * {@link #outboxAdminRestSecurityCheck} fail-fasts at startup when Spring Security is present but
 * method security is not active. Applications without Spring Security run the API unprotected by
 * design; {@code event-outboxer.admin.rest.enforce-authority=false} opts out of the check
 * explicitly.
 */
@AutoConfiguration
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnProperty(prefix = "event-outboxer.admin.rest", name = "enabled", havingValue = "true")
// No classes listed on purpose: registers the binding infrastructure only, while the properties
// bean itself is declared below under the stable name the controller's SpEL dereferences.
@EnableConfigurationProperties
public class OutboxAdminRestAutoConfiguration {

  private static final String SECURITY_MARKER_CLASS =
      "org.springframework.security.core.context.SecurityContextHolder";

  /**
   * Bean name registered by {@code @EnableMethodSecurity}'s {@code
   * PrePostMethodSecurityConfiguration}. Detection is name-based on purpose: the bean is declared
   * with the {@code MethodInterceptor} interface return type and a lazy wrapper instance, so
   * type-based lookups cannot identify it.
   */
  private static final String PRE_AUTHORIZE_INTERCEPTOR_BEAN =
      "preAuthorizeAuthorizationMethodInterceptor";

  /**
   * Explicit {@code @Bean} instead of {@code @EnableConfigurationProperties} so the bean gets the
   * stable name {@code outboxAdminRestProperties} that the controller's {@code @PreAuthorize} SpEL
   * dereferences. Binding still happens through the standard {@code @ConfigurationProperties}
   * post-processing.
   */
  @Bean
  @ConditionalOnMissingBean
  public OutboxAdminRestProperties outboxAdminRestProperties() {
    return new OutboxAdminRestProperties();
  }

  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnBean({OutboxAdmin.class, EventStore.class})
  public OutboxAdminController outboxAdminController(OutboxAdmin admin, EventStore store) {
    return new OutboxAdminController(admin, store);
  }

  @Bean
  public SmartInitializingSingleton outboxAdminRestSecurityCheck(
      ListableBeanFactory beanFactory, OutboxAdminRestProperties properties) {
    return () -> {
      if (!properties.isEnforceAuthority()) {
        return;
      }
      ClassLoader cl = OutboxAdminRestAutoConfiguration.class.getClassLoader();
      if (!ClassUtils.isPresent(SECURITY_MARKER_CLASS, cl)) {
        // No Spring Security at all: @PreAuthorize is inert and the API runs open — an
        // accepted trade-off for security-less applications (ADR-0019).
        return;
      }
      if (!beanFactory.containsBean(PRE_AUTHORIZE_INTERCEPTOR_BEAN)) {
        throw new IllegalStateException(
            "event-outboxer.admin.rest.enabled=true and Spring Security is on the classpath, "
                + "but method security is not active — the controller's @PreAuthorize guard "
                + "would be silently ignored and the admin API would run unprotected. "
                + "Add @EnableMethodSecurity to your security configuration, or set "
                + "event-outboxer.admin.rest.enforce-authority=false to explicitly accept an "
                + "unprotected admin API.");
      }
    };
  }
}
