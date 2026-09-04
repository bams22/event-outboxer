/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.target.outboxer;

import java.util.List;

/**
 * Where the worker instances of the event-outboxer target live: Spring contexts in this JVM or
 * forked JVMs. Ids are handed out by the fleet ({@code bench-w<n>}) and never reused within a run.
 */
interface WorkerFleet extends AutoCloseable {

    /** Boots {@code count} workers and returns their ids once each is ready to poll. */
    List<String> start(int count);

    /** Ids of running workers, oldest first. */
    List<String> ids();

    /**
     * Kills {@code count} running workers without any grace, oldest first.
     *
     * @throws UnsupportedOperationException when this fleet cannot do that honestly
     */
    List<String> kill(int count);

    /** Stops every running worker gracefully. */
    @Override
    void close();
}
