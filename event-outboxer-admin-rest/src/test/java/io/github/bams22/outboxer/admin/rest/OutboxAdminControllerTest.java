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

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryOutboxAdmin;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/**
 * Functional behaviour of the admin controller (standalone MockMvc — method security is covered
 * separately by {@link OutboxAdminSecurityTest}).
 */
class OutboxAdminControllerTest {

    private static final WorkerId WORKER = new WorkerId("rest-test");

    private InMemoryEventStore store;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        store = new InMemoryEventStore();
        mvc =
                MockMvcBuilders.standaloneSetup(
                                new OutboxAdminController(new InMemoryOutboxAdmin(store), store))
                        .build();
    }

    @Test
    @DisplayName("GET /events lists the DISABLED backlog with a cursor on full pages")
    void listsDisabled() throws Exception {
        for (int i = 0; i < 3; i++) {
            disableOne("T", "p-" + i);
        }

        mvc.perform(get("/outbox-admin/events").param("limit", "3"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.events", hasSize(3)))
                .andExpect(jsonPath("$.nextCursor", notNullValue()));
        mvc.perform(get("/outbox-admin/events").param("limit", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.events", hasSize(3)))
                .andExpect(jsonPath("$.nextCursor", nullValue()));
    }

    @Test
    @DisplayName("GET /events/{id}: 200 for active, 404 for unknown")
    void readsSingle() throws Exception {
        UUID id = disableOne("T", "single");

        mvc.perform(get("/outbox-admin/events/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("DISABLED"));
        mvc.perform(get("/outbox-admin/events/{id}", UUID.randomUUID()))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("POST /events/{id}/reenable: 200 / 409 / 404")
    void reenableStatusCodes() throws Exception {
        UUID disabled = disableOne("T", "revive");
        PendingEvent pendingEvent = pendingEvent("T", "still-pending");
        store.save(pendingEvent);

        mvc.perform(post("/outbox-admin/events/{id}/reenable", disabled))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").value(1));
        mvc.perform(post("/outbox-admin/events/{id}/reenable", pendingEvent.id()))
                .andExpect(status().isConflict());
        mvc.perform(post("/outbox-admin/events/{id}/reenable", UUID.randomUUID()))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("bulk endpoints: reenable-all and purge/disabled return counts")
    void bulkEndpoints() throws Exception {
        disableOne("A", "a1");
        disableOne("A", "a2");
        disableOne("B", "b1");

        mvc.perform(
                        post("/outbox-admin/events/reenable-all")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content("{\"eventType\": \"A\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").value(2));
        mvc.perform(
                        post("/outbox-admin/purge/disabled")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(
                                        "{\"olderThan\": \""
                                                + Instant.now().plusSeconds(3600)
                                                + "\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").value(1));
    }

    private UUID disableOne(String type, String payload) {
        PendingEvent p = pendingEvent(type, payload);
        store.save(p);
        ClaimedEvent claimed =
                store.claim(new ClaimRequest(type, WORKER, 100)).stream()
                        .filter(ce -> ce.id().equals(p.id()))
                        .findFirst()
                        .orElseThrow();
        store.markDisabled(claimed.id(), WORKER, claimed.claimedVersion(), "test");
        return p.id();
    }

    private static PendingEvent pendingEvent(String type, String payload) {
        return PendingEvent.builder()
                .id(UUID.randomUUID())
                .eventType(type)
                .payload(SerializedPayload.ofText("\"" + payload + "\""))
                .payloadFormat("test-json")
                .payloadClass("java.lang.String")
                .priority((short) 0)
                .runAt(Instant.now().minusSeconds(1))
                .traceContext(Map.of())
                .build();
    }
}
