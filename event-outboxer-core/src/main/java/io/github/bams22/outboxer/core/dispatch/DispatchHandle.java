/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.dispatch;

import java.util.Objects;

/**
 * Cancellation handle for one in-flight dispatch, bound to the thread executing it.
 *
 * <p>The watchdog uses it to interrupt a handler whose row it has just force-reclaimed: from that
 * moment the handler's outcome is discarded by the optimistic-lock check anyway (ADR-0014), so
 * letting it keep running only wastes a slot of the per-type handler pool.
 *
 * <p>The handle wraps the dispatching {@link Thread} rather than the executor {@code Future}: the
 * registry only ever holds dispatches that have already started, for which {@code
 * Future.cancel(true)} does nothing beyond the same interrupt — and a thread reference needs no
 * plumbing through the poller and the executor gate, and behaves identically for platform and
 * virtual threads.
 *
 * <p>Thread-safety: {@link #interruptIfActive()} and {@link #deactivate()} are mutually exclusive,
 * so an interrupt can never land on a pool thread that has already moved on to the next event. The
 * dispatcher must call {@code deactivate()} before removing itself from the {@link
 * InFlightRegistry}, and must call {@link #consumeInterrupt()} from its own thread so a watchdog
 * cancellation neither breaks the dispatch's cleanup nor leaks into the next event. Only the
 * interrupts this handle delivered are consumed — the thread's interrupt status is shared with
 * everything else running on it, {@code shutdownNow()} included.
 *
 * <p>The handle also serves as the identity of one dispatch: the abandoned set in {@link
 * InFlightRegistry} is keyed by it, because the same event id can already be running again on
 * another thread of this JVM while the force-reclaimed dispatch is still going.
 */
public final class DispatchHandle {

    private final Thread thread;
    private boolean active = true;
    private boolean interrupted;

    public DispatchHandle(Thread thread) {
        this.thread = Objects.requireNonNull(thread, "thread must not be null");
    }

    /**
     * Interrupt the dispatching thread if it is still running this dispatch.
     *
     * @return {@code true} if the interrupt was delivered, {@code false} if the dispatch had
     *     already finished
     */
    public synchronized boolean interruptIfActive() {
        if (!active) {
            return false;
        }
        thread.interrupt();
        interrupted = true;
        return true;
    }

    /**
     * Detach the handle from its thread; called from the dispatch's {@code finally} block. Once it
     * returns, {@link #interruptIfActive()} can no longer fire, so a {@link #consumeInterrupt()}
     * after it is final.
     */
    public synchronized void deactivate() {
        active = false;
    }

    /**
     * Clear a watchdog-issued interrupt on the dispatching thread. Must be called from that thread,
     * and is meant to be called repeatedly — the dispatcher consumes the interrupt as soon as the
     * handler has unwound (so it cannot break the finalize and lock-release that follow) and once
     * more before returning the thread to the pool, by which point {@link #deactivate()} has ruled
     * out any further interrupt.
     *
     * <p>Consumes each interrupt exactly once, so it only ever clears what this handle caused: an
     * interrupt a handler set on itself, or one {@code shutdownNow()} delivered afterwards, is none
     * of this class's business and is left standing.
     */
    public void consumeInterrupt() {
        boolean fired;
        synchronized (this) {
            fired = interrupted;
            interrupted = false;
        }
        if (fired) {
            Thread.interrupted();
        }
    }

    /** Whether the dispatch this handle belongs to is still running. */
    public synchronized boolean isActive() {
        return active;
    }

    /** Name of the thread running the dispatch — the useful bit when reporting a leaked thread. */
    public String threadName() {
        return thread.getName();
    }
}
