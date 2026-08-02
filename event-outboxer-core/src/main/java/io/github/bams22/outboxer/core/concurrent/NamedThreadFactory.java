/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.concurrent;

import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ThreadFactory} producing daemon threads with a stable {@code prefix-N} name so that
 * pollers, handler pools and maintenance tasks are distinguishable in thread dumps.
 */
public final class NamedThreadFactory implements ThreadFactory {

    private static final Logger log = LoggerFactory.getLogger(NamedThreadFactory.class);

    private final ThreadFactory delegate;

    public NamedThreadFactory(String prefix) {
        this(prefix, true);
    }

    public NamedThreadFactory(String prefix, boolean daemon) {
        Objects.requireNonNull(prefix, "prefix must not be null");
        this.delegate =
                Thread.ofPlatform()
                        .name(prefix + "-", 1)
                        .daemon(daemon)
                        .uncaughtExceptionHandler(
                                (thread, throwable) ->
                                        log.error(
                                                "uncaught exception in {}: {}",
                                                thread.getName(),
                                                throwable.toString(),
                                                throwable))
                        .factory();
    }

    @Override
    public Thread newThread(Runnable r) {
        return delegate.newThread(r);
    }
}
