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
import io.github.bams22.outboxer.spi.ArchiveCursor;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.spi.OutboxAdmin.ReplayAllResult;
import io.github.bams22.outboxer.spi.OutboxAdmin.ReplayOutcome;
import io.github.bams22.outboxer.spi.contracts.support.StubOutboxAdmin;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryOutboxAdmin;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.jspecify.annotations.Nullable;
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
    private static final Instant CURSOR_AT = Instant.parse("2026-02-01T10:15:30.123456Z");
    private static final UUID NEXT_ID = UUID.fromString("0f9a2c31-1111-4222-8333-444455556666");
    private static final ReplayAllResult EMPTY_BULK = new ReplayAllResult(0, 0, 0, null);

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
    @DisplayName("GET /events/{id}: 200 for active (dedupKey included), 404 for unknown")
    void readsSingle() throws Exception {
        PendingEvent keyed = pendingEvent("T", "single", "rest-key");
        store.save(keyed);

        mvc.perform(get("/outbox-admin/events/{id}", keyed.id()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.dedupKey").value("rest-key"));
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

    @Test
    @DisplayName("replay endpoints: 404 / zero counts on the archive-less in-memory adapter")
    void replayEndpointsOnInMemory() throws Exception {
        mvc.perform(post("/outbox-admin/events/{id}/replay", UUID.randomUUID()))
                .andExpect(status().isNotFound());
        mvc.perform(
                        post("/outbox-admin/events/replay-all")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content("{\"eventType\": \"A\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.replayed").value(0))
                .andExpect(jsonPath("$.coalesced").value(0))
                .andExpect(jsonPath("$.idInUse").value(0))
                .andExpect(jsonPath("$.nextCursor").doesNotExist());
    }

    @Test
    @DisplayName("replay endpoints map REPLAYED/COALESCED outcomes to 200 bodies")
    void replayEndpointsOutcomeMapping() throws Exception {
        ReplayStubAdmin stub =
                new ReplayStubAdmin(
                        ReplayOutcome.REPLAYED,
                        new ReplayAllResult(3, 2, 1, new ArchiveCursor(CURSOR_AT, NEXT_ID)));
        MockMvc stubbed = stubbedMvc(stub);

        stubbed.perform(post("/outbox-admin/events/{id}/replay", UUID.randomUUID()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.outcome").value("REPLAYED"));
        stubbed.perform(
                        post("/outbox-admin/events/replay-all")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content("{\"eventType\": \"A\", \"limit\": 5}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.replayed").value(3))
                .andExpect(jsonPath("$.coalesced").value(2))
                .andExpect(jsonPath("$.idInUse").value(1))
                .andExpect(jsonPath("$.nextCursor").value(CURSOR_AT + "_" + NEXT_ID));
    }

    @Test
    @DisplayName("a replay whose id is already live maps to 409, not to a 200 outcome body")
    void replayIdInUseIsConflict() throws Exception {
        stubbedMvc(new ReplayStubAdmin(ReplayOutcome.ID_IN_USE, EMPTY_BULK))
                .perform(post("/outbox-admin/events/{id}/replay", UUID.randomUUID()))
                .andExpect(status().isConflict())
                .andExpect(
                        jsonPath("$.error")
                                .value("an event with this id is already in the outbox"));
    }

    @Test
    @DisplayName("replay-all decodes the request cursor and hands it to the adapter")
    void replayAllPassesCursorThrough() throws Exception {
        ReplayStubAdmin stub = new ReplayStubAdmin(ReplayOutcome.REPLAYED, EMPTY_BULK);

        stubbedMvc(stub)
                .perform(
                        post("/outbox-admin/events/replay-all")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(
                                        "{\"eventType\": \"A\", \"cursor\": \""
                                                + CURSOR_AT
                                                + "_"
                                                + NEXT_ID
                                                + "\"}"))
                .andExpect(status().isOk());

        assertThat(stub.cursorsSeen).containsExactly(new ArchiveCursor(CURSOR_AT, NEXT_ID));
    }

    @Test
    @DisplayName("a rejected argument is a 400 with the reason, not a 500")
    void badArgumentsAreBadRequest() throws Exception {
        // Every one of these reaches the controller as an IllegalArgumentException: from the DTO
        // layer (cursor) and from the OutboxAdmin port (window, limit). Without the handler they
        // would all be 500s, indistinguishable from a broken store.
        mvc.perform(
                        post("/outbox-admin/events/replay-all")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(
                                        "{\"eventType\": \"A\","
                                                + " \"archivedAfter\": \"2026-01-01T12:00:00Z\","
                                                + " \"archivedBefore\":"
                                                + " \"2026-01-01T10:00:00Z\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(
                        jsonPath("$.error")
                                .value(
                                        org.hamcrest.Matchers.containsString(
                                                "archivedAfter must be strictly before"
                                                        + " archivedBefore")));

        mvc.perform(
                        post("/outbox-admin/events/replay-all")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content("{\"eventType\": \"A\", \"limit\": -1}"))
                .andExpect(status().isBadRequest());

        mvc.perform(get("/outbox-admin/events").param("cursor", "not-a-cursor"))
                .andExpect(status().isBadRequest())
                .andExpect(
                        jsonPath("$.error")
                                .value(org.hamcrest.Matchers.containsString("malformed cursor")));
    }

    private MockMvc stubbedMvc(ReplayStubAdmin stub) {
        return MockMvcBuilders.standaloneSetup(new OutboxAdminController(stub, store)).build();
    }

    private UUID disableOne(String type, String payload) {
        PendingEvent p = pendingEvent(type, payload, null);
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
        return pendingEvent(type, payload, null);
    }

    private static PendingEvent pendingEvent(
            String type, String payload, @Nullable String dedupKey) {
        return PendingEvent.builder()
                .id(UUID.randomUUID())
                .eventType(type)
                .payload(SerializedPayload.ofText("\"" + payload + "\""))
                .payloadFormat("test-json")
                .payloadClass("java.lang.String")
                .priority((short) 0)
                .runAt(Instant.now().minusSeconds(1))
                .traceContext(Map.of())
                .dedupKey(dedupKey)
                .build();
    }

    /**
     * Replay outcomes the archive-less in-memory adapter cannot produce (it only ever answers
     * NOT_FOUND/zeros), so the controller's status and body mapping can be driven directly.
     */
    private static final class ReplayStubAdmin extends StubOutboxAdmin {

        private final ReplayOutcome outcome;
        private final ReplayAllResult bulk;
        private final List<@Nullable ArchiveCursor> cursorsSeen = new ArrayList<>();

        ReplayStubAdmin(ReplayOutcome outcome, ReplayAllResult bulk) {
            this.outcome = outcome;
            this.bulk = bulk;
        }

        @Override
        public ReplayOutcome replayFromArchive(UUID id) {
            return outcome;
        }

        @Override
        public ReplayAllResult replayAllFromArchive(
                String eventType,
                @Nullable Instant archivedAfter,
                @Nullable Instant archivedBefore,
                int limit,
                @Nullable ArchiveCursor after) {
            cursorsSeen.add(after);
            return bulk;
        }
    }
}
