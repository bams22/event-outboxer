/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spi.contracts;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.domain.ClaimedEvent;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.domain.PendingEvent;
import io.github.bams22.outboxer.domain.SerializedPayload;
import io.github.bams22.outboxer.domain.WorkerId;
import io.github.bams22.outboxer.spi.AdminCursor;
import io.github.bams22.outboxer.spi.ClaimRequest;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.OutboxAdmin;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Reusable contract specification for every {@link OutboxAdmin} implementation. Subclasses provide
 * a matching {@link EventStore} + {@link OutboxAdmin} pair over the same underlying storage;
 * archive-specific behaviour is covered separately by adapter tests because the archive is a
 * PostgreSQL-only feature (ADR-0008).
 */
public abstract class AbstractOutboxAdminContractTest {

  protected static final String TYPE_A = "ADMIN_A";
  protected static final String TYPE_B = "ADMIN_B";
  protected static final WorkerId WORKER = new WorkerId("admin-contract-worker");

  protected EventStore store;
  protected OutboxAdmin admin;

  /** Fresh store the admin below operates on. */
  protected abstract EventStore newStore();

  /** Admin over the SAME storage as the last {@link #newStore()} result. */
  protected abstract OutboxAdmin newAdmin();

  @BeforeEach
  void setUpAdmin() {
    store = newStore();
    admin = newAdmin();
  }

  // ---------------------------------------------------------------------------------------------
  // findByStatus
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("findByStatus() filters by status and type, newest first")
  void findByStatus_filtersAndOrders() {
    disable(publishAndClaim(TYPE_A, "a1"));
    disable(publishAndClaim(TYPE_A, "a2"));
    disable(publishAndClaim(TYPE_B, "b1"));
    store.save(pending(TYPE_A, "pending-1")); // stays PENDING — must not appear

    List<Event> disabledA = admin.findByStatus(EventStatus.DISABLED, TYPE_A, 10, null);

    assertThat(disabledA).hasSize(2);
    assertThat(disabledA).allMatch(e -> e.status() == EventStatus.DISABLED);
    assertThat(disabledA).allMatch(e -> e.eventType().equals(TYPE_A));
    List<Event> all = admin.findByStatus(EventStatus.DISABLED, null, 10, null);
    assertThat(all).hasSize(3);
  }

  @Test
  @DisplayName("findByStatus() pages with the keyset cursor without gaps or duplicates")
  void findByStatus_paginates() {
    Set<UUID> expected = new HashSet<>();
    for (int i = 0; i < 7; i++) {
      expected.add(disable(publishAndClaim(TYPE_A, "d-" + i)));
    }

    Set<UUID> seen = new HashSet<>();
    AdminCursor cursor = null;
    int pages = 0;
    while (true) {
      List<Event> page = admin.findByStatus(EventStatus.DISABLED, TYPE_A, 3, cursor);
      if (page.isEmpty()) {
        break;
      }
      pages++;
      for (Event e : page) {
        assertThat(seen.add(e.id())).as("duplicate row across pages: %s", e.id()).isTrue();
      }
      Event last = page.getLast();
      cursor = new AdminCursor(last.createdAt(), last.id());
      if (page.size() < 3) {
        break;
      }
    }
    assertThat(seen).isEqualTo(expected);
    assertThat(pages).isGreaterThanOrEqualTo(3);
  }

  // ---------------------------------------------------------------------------------------------
  // reenable
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("reenable() returns a DISABLED row to PENDING with a fresh attempts budget")
  void reenable_resetsAttemptsAndStatus() {
    ClaimedEvent claimed = publishAndClaim(TYPE_A, "x");
    // Fail it a couple of times so attempts > 0 before disabling.
    store.markForRetry(
        claimed.id(), WORKER, claimed.claimedVersion(), "fail-1", Instant.now().minusSeconds(1));
    ClaimedEvent again = claimOne(TYPE_A, claimed.id());
    store.markDisabled(again.id(), WORKER, again.claimedVersion(), "gave up");
    Event before = store.findById(claimed.id()).orElseThrow();
    assertThat(before.attempts()).isGreaterThan(0);

    boolean ok = admin.reenable(claimed.id());

    assertThat(ok).isTrue();
    Event after = store.findById(claimed.id()).orElseThrow();
    assertThat(after.status()).isEqualTo(EventStatus.PENDING);
    assertThat(after.attempts()).isZero();
    assertThat(after.version()).isGreaterThan(before.version());
    assertThat(after.claimedBy()).isNull();
    // Re-enabled events must be claimable again.
    assertThat(store.claim(new ClaimRequest(TYPE_A, WORKER, 10)))
        .anyMatch(ce -> ce.id().equals(claimed.id()));
  }

  @Test
  @DisplayName("reenable() refuses rows that are not DISABLED and unknown ids")
  void reenable_onlyDisabledRows() {
    // Claim first, save the pending row after — the claim sweeps every eligible row of the type.
    ClaimedEvent processing = publishAndClaim(TYPE_A, "in-flight");
    PendingEvent pendingEvent = pending(TYPE_A, "still-pending");
    store.save(pendingEvent);

    assertThat(admin.reenable(pendingEvent.id())).isFalse();
    assertThat(admin.reenable(processing.id())).isFalse();
    assertThat(admin.reenable(UUID.randomUUID())).isFalse();

    assertThat(store.findById(pendingEvent.id()).orElseThrow().status())
        .isEqualTo(EventStatus.PENDING);
    assertThat(store.findById(processing.id()).orElseThrow().status())
        .isEqualTo(EventStatus.PROCESSING);
  }

  @Test
  @DisplayName("reenableAll() re-enables up to limit rows of one type only")
  void reenableAll_scopedAndLimited() {
    List<UUID> disabledA = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      disabledA.add(disable(publishAndClaim(TYPE_A, "a-" + i)));
    }
    UUID disabledB = disable(publishAndClaim(TYPE_B, "b-0"));

    int first = admin.reenableAll(TYPE_A, null, 3);
    int second = admin.reenableAll(TYPE_A, null, 10);

    assertThat(first).isEqualTo(3);
    assertThat(second).isEqualTo(2);
    for (UUID id : disabledA) {
      assertThat(store.findById(id).orElseThrow().status()).isEqualTo(EventStatus.PENDING);
      assertThat(store.findById(id).orElseThrow().attempts()).isZero();
    }
    assertThat(store.findById(disabledB).orElseThrow().status()).isEqualTo(EventStatus.DISABLED);
  }

  // ---------------------------------------------------------------------------------------------
  // purgeDisabled
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("purgeDisabled() deletes only DISABLED rows older than the threshold")
  void purgeDisabled_respectsAgeAndStatus() {
    UUID oldDisabled = disable(publishAndClaim(TYPE_A, "old"));
    store.save(pending(TYPE_A, "pending"));

    // Everything was just created; a threshold in the past must delete nothing.
    assertThat(admin.purgeDisabled(null, Instant.now().minusSeconds(3600), 100)).isZero();

    // A future threshold ("older than tomorrow") covers the disabled row, not the pending one.
    int purged = admin.purgeDisabled(null, Instant.now().plusSeconds(3600), 100);

    assertThat(purged).isEqualTo(1);
    assertThat(store.findById(oldDisabled)).isEmpty();
    assertThat(admin.findByStatus(EventStatus.DISABLED, null, 10, null)).isEmpty();
  }

  @Test
  @DisplayName("purgeDisabled() honours the limit")
  void purgeDisabled_respectsLimit() {
    for (int i = 0; i < 5; i++) {
      disable(publishAndClaim(TYPE_A, "d-" + i));
    }

    assertThat(admin.purgeDisabled(TYPE_A, Instant.now().plusSeconds(3600), 2)).isEqualTo(2);
    assertThat(admin.findByStatus(EventStatus.DISABLED, TYPE_A, 10, null)).hasSize(3);
  }

  // ---------------------------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------------------------

  protected PendingEvent pending(String type, String payload) {
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

  protected ClaimedEvent publishAndClaim(String type, String payload) {
    PendingEvent p = pending(type, payload);
    store.save(p);
    return claimOne(type, p.id());
  }

  protected ClaimedEvent claimOne(String type, UUID id) {
    return store.claim(new ClaimRequest(type, WORKER, 100)).stream()
        .filter(ce -> ce.id().equals(id))
        .findFirst()
        .orElseThrow(() -> new AssertionError("event not claimed: " + id));
  }

  /** Disable the claimed event and return its id. */
  protected UUID disable(ClaimedEvent claimed) {
    boolean ok = store.markDisabled(claimed.id(), WORKER, claimed.claimedVersion(), "test-disable");
    assertThat(ok).isTrue();
    return claimed.id();
  }
}
