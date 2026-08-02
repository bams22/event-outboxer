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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryOutboxAdmin;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.core.context.SecurityContextHolder;

/**
 * The {@code @PreAuthorize} guard with the property-driven authority: enforced through method
 * security, permit name read from the {@code outboxAdminRestProperties} bean via SpEL.
 */
class OutboxAdminSecurityTest {

    private AnnotationConfigApplicationContext context;
    private OutboxAdminController controller;

    @BeforeEach
    void setUp() {
        context = new AnnotationConfigApplicationContext(SecuredConfig.class);
        controller = context.getBean(OutboxAdminController.class);
    }

    @AfterEach
    void tearDown() {
        SecurityContextHolder.clearContext();
        context.close();
    }

    @Test
    @DisplayName("without the configured authority → AccessDeniedException")
    void deniedWithoutAuthority() {
        authenticate("SOME_OTHER_AUTHORITY");

        assertThatThrownBy(() -> controller.events(EventStatus.DISABLED, null, 10, null))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> controller.reenable(UUID.randomUUID()))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    @DisplayName("with the property-configured authority → allowed")
    void allowedWithConfiguredAuthority() {
        authenticate("CUSTOM_PERMIT"); // matches the property below, not the default

        assertThat(controller.events(EventStatus.DISABLED, null, 10, null).events()).isEmpty();
    }

    @Test
    @DisplayName("unauthenticated → denied")
    void deniedWhenUnauthenticated() {
        SecurityContextHolder.clearContext();

        assertThatThrownBy(() -> controller.events(EventStatus.DISABLED, null, 10, null))
                .isInstanceOf(
                        RuntimeException.class); // AuthenticationCredentialsNotFound / AccessDenied
    }

    private static void authenticate(String... authorities) {
        TestingAuthenticationToken auth =
                new TestingAuthenticationToken("admin-user", "n/a", authorities);
        auth.setAuthenticated(true);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

    @Configuration
    @EnableMethodSecurity
    static class SecuredConfig {

        @Bean
        OutboxAdminRestProperties outboxAdminRestProperties() {
            OutboxAdminRestProperties props = new OutboxAdminRestProperties();
            props.setRequiredAuthority("CUSTOM_PERMIT");
            return props;
        }

        @Bean
        OutboxAdminController outboxAdminController() {
            InMemoryEventStore store = new InMemoryEventStore();
            return new OutboxAdminController(new InMemoryOutboxAdmin(store), store);
        }
    }
}
