/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.domain.exception;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ExceptionCodesTest {

    @Test
    void publishValidationCarriesCode() {
        PublishValidationException e = new PublishValidationException("missing eventType");
        assertThat(e)
                .hasMessageStartingWith("OUTBOX-101:")
                .hasMessageContaining("missing eventType");
    }

    @Test
    void publishSerializationCarriesCauseAndCode() {
        Throwable cause = new RuntimeException("boom");
        PublishSerializationException e =
                new PublishSerializationException("serialize failed", cause);
        assertThat(e).hasMessageStartingWith("OUTBOX-102:").hasCause(cause);
    }

    @Test
    void noTransactionCarriesCode() {
        assertThat(new NoTransactionException("no TX"))
                .hasMessageStartingWith("OUTBOX-103:")
                .hasMessageContaining("no TX");
    }

    @Test
    void publishFailedCarriesCodeAndCause() {
        Throwable cause = new IllegalStateException();
        assertThat(new PublishFailedException("batch failed", cause))
                .hasMessageStartingWith("OUTBOX-104:")
                .hasCause(cause);
    }

    @Test
    void unknownEventTypeCarriesCodeAndEventType() {
        UnknownEventTypeException e = new UnknownEventTypeException("UNKNOWN_X");
        assertThat(e).hasMessageStartingWith("OUTBOX-201:").hasMessageContaining("UNKNOWN_X");
        assertThat(e.eventType()).isEqualTo("UNKNOWN_X");
    }

    @Test
    void payloadDeserializationCarriesCode() {
        assertThat(new PayloadDeserializationException("bad payload", new RuntimeException()))
                .hasMessageStartingWith("OUTBOX-202:");
    }

    @Test
    void storageCodes() {
        assertThat(new EventStoreException("sql error", new RuntimeException()))
                .hasMessageStartingWith("OUTBOX-302:");
        assertThat(new WorkerRegistryException("heartbeat failed", new RuntimeException()))
                .hasMessageStartingWith("OUTBOX-303:");
    }

    @Test
    void lockCodes() {
        assertThat(new LockAcquisitionException("redis down", new RuntimeException()))
                .hasMessageStartingWith("OUTBOX-401:");
        assertThat(new LockReleaseException("lua script failed", new RuntimeException()))
                .hasMessageStartingWith("OUTBOX-402:");
    }

    @Test
    void categoryHierarchyIsTraversable() {
        // Catching the category base is enough to handle every concrete subtype.
        OutboxException publish = new PublishValidationException("x");
        assertThat(publish)
                .isInstanceOf(PublishException.class)
                .isInstanceOf(OutboxException.class);

        OutboxException storage = new EventStoreException("y", new RuntimeException());
        assertThat(storage)
                .isInstanceOf(StorageException.class)
                .isInstanceOf(OutboxException.class);
    }
}
