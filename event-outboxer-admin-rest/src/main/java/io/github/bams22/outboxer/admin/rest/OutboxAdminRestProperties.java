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

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration of the opt-in admin REST surface (ADR-0019). Registered under the well-known
 * bean name {@code outboxAdminRestProperties} — the controller's {@code @PreAuthorize} SpEL
 * references it to read the required authority, which keeps the permit name fully
 * user-configurable instead of hard-coded.
 */
@Getter
@Setter
@ConfigurationProperties("event-outboxer.admin.rest")
public class OutboxAdminRestProperties {

  /**
   * Master switch. {@code false} (default): no controller is registered. A write-capable HTTP
   * surface must be a deliberate decision.
   */
  private boolean enabled = false;

  /** Base path of the admin API. */
  private String basePath = "/outbox-admin";

  /** Authority required on the authenticated principal for every admin operation. */
  private String requiredAuthority = "OUTBOX_ADMIN";

  /**
   * When {@code true} (default) and Spring Security is on the classpath, startup fails unless
   * method security ({@code @EnableMethodSecurity}) is active — otherwise {@code @PreAuthorize}
   * would be silently ignored and the API would run unprotected while appearing secured. Set to
   * {@code false} to explicitly accept an unprotected admin API (dev environments).
   */
  private boolean enforceAuthority = true;
}
