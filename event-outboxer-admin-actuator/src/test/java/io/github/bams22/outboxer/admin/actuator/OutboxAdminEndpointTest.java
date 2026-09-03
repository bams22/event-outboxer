/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.admin.actuator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.storage.inmemory.InMemoryEventStore;
import io.github.bams22.outboxer.storage.inmemory.InMemoryOutboxAdmin;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class OutboxAdminEndpointTest {

    private static final WorkerId WORKER = new WorkerId("actuator-test");

    private InMemoryEventStore store;
    private OutboxAdminEndpoint endpoint;

    @BeforeEach
    void setUp() {
        store = new InMemoryEventStore();
        endpoint = new OutboxAdminEndpoint(new InMemoryOutboxAdmin(store), store);
    }

    @Test
    @DisplayName("events() defaults to the DISABLED backlog and pages via the cursor")
    void listsDisabledWithCursor() {
        for (int i = 0; i < 5; i++) {
            disableOne("T", "p-" + i);
        }

        Map<String, Object> first = endpoint.events(null, null, 3, null);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> firstPage = (List<Map<String, Object>>) first.get("events");
        assertThat(firstPage).hasSize(3);
        assertThat(first.get("nextCursor")).isNotNull();

        Map<String, Object> second =
                endpoint.events(null, null, 3, (String) first.get("nextCursor"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> secondPage = (List<Map<String, Object>>) second.get("events");
        assertThat(secondPage).hasSize(2);
        assertThat(second.get("nextCursor")).isNull();
    }

    @Test
    @DisplayName("event(id) reads the active row; unknown ids yield null (404)")
    void readsSingleEvent() {
        UUID id = disableOne("T", "single");

        Map<String, Object> body = endpoint.event(id.toString());

        assertThat(body).isNotNull();
        assertThat(body.get("status")).isEqualTo("DISABLED");
        assertThat(endpoint.event(UUID.randomUUID().toString())).isNull();
    }

    @Test
    @DisplayName("reenable(id) flips a DISABLED event back to PENDING")
    void reenablesSingleEvent() {
        UUID id = disableOne("T", "revive-me");

        Map<String, Object> result = endpoint.reenable(id.toString(), null);

        assertThat(result.get("reenabled")).isEqualTo(true);
        assertThat(store.findById(id).orElseThrow().status()).isEqualTo(EventStatus.PENDING);
    }

    @Test
    @DisplayName("reenableAll and purge operate per type / target with limits")
    void bulkOperations() {
        disableOne("A", "a1");
        disableOne("A", "a2");
        disableOne("B", "b1");

        assertThat(endpoint.reenableAll("A", null, null, null, null, null).get("reenabled"))
                .isEqualTo(2);
        assertThat(endpoint.purge("disabled", 0, null, null).get("purged")).isEqualTo(1); // B only
        assertThatThrownBy(() -> endpoint.purge("bogus", 1, null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("action=replay routes to the replay operations; no archive in-memory → NOT_FOUND")
    void replayActions() {
        assertThat(endpoint.reenable(UUID.randomUUID().toString(), "replay").get("outcome"))
                .isEqualTo("NOT_FOUND");
        Map<String, @Nullable Object> bulk =
                endpoint.reenableAll("A", null, "replay", null, null, null);
        assertThat(bulk.get("replayed")).isEqualTo(0);
        assertThat(bulk.get("coalesced")).isEqualTo(0);
        assertThat(bulk.get("idInUse")).isEqualTo(0);
        // No archive means nothing was considered, so the sweep ends immediately.
        assertThat(bulk).containsEntry("nextCursor", null);
        // The window parameters are parsed as ISO instants on the replay path.
        assertThat(
                        endpoint.reenableAll(
                                        "A",
                                        null,
                                        "replay",
                                        "2026-01-01T00:00:00Z",
                                        "2026-02-01T00:00:00Z",
                                        null)
                                .get("replayed"))
                .isEqualTo(0);
        // A cursor round-trips through the opaque <instant>_<uuid> form.
        assertThat(
                        endpoint.reenableAll(
                                        "A",
                                        null,
                                        "replay",
                                        null,
                                        null,
                                        "2026-01-01T00:00:00Z_"
                                                + "0f9a2c31-1111-4222-8333-444455556666")
                                .get("replayed"))
                .isEqualTo(0);
    }

    @Test
    @DisplayName("bogus action and a window without action=replay are rejected")
    void replayActionValidation() {
        assertThatThrownBy(() -> endpoint.reenable(UUID.randomUUID().toString(), "bogus"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> endpoint.reenableAll("A", null, "bogus", null, null, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                endpoint.reenableAll(
                                        "A", null, null, "2026-01-01T00:00:00Z", null, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> endpoint.reenableAll("A", null, null, null, null, "c"))
                .isInstanceOf(IllegalArgumentException.class);
        // A malformed instant is a client error, not a 500: Actuator renders
        // IllegalArgumentException as HTTP 400, DateTimeParseException as HTTP 500.
        assertThatThrownBy(() -> endpoint.reenableAll("A", null, "replay", "yesterday", null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("archivedAfter");
    }

    private UUID disableOne(String type, String payload) {
        PendingEvent p =
                PendingEvent.builder()
                        .id(UUID.randomUUID())
                        .eventType(type)
                        .payload(SerializedPayload.ofText("\"" + payload + "\""))
                        .payloadFormat("test-json")
                        .payloadClass("java.lang.String")
                        .priority((short) 0)
                        .runAt(Instant.now().minusSeconds(1))
                        .traceContext(Map.of())
                        .build();
        store.save(p);
        ClaimedEvent claimed =
                store.claim(new ClaimRequest(type, WORKER, 100)).stream()
                        .filter(ce -> ce.id().equals(p.id()))
                        .findFirst()
                        .orElseThrow();
        store.markDisabled(claimed.id(), WORKER, claimed.claimedVersion(), "test");
        return p.id();
    }
}
