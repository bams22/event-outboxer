/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.api.handle;

/**
 * Strategy for deciding what to do when an event handler fails — returns a non-success outcome or
 * lets an exception escape. Designed as a chain-of-responsibility (see ADR-0007): each
 * implementation in the {@code builtin} package decorates another {@code FailureHandler},
 * optionally reacting to the failure (logging, metrics, listener callbacks) and delegating down to
 * the next handler in the chain. A <em>leaf</em> handler such as {@code
 * ExponentialBackoffFailureHandler} or {@code NoRetryFailureHandler} makes the final decision.
 *
 * <p>Priority order for resolving which failure handler applies to a given event:
 *
 * <ol>
 *   <li>{@code EventHandler.failureHandler()} if non-null.
 *   <li>The per-type failure handler registered for the event type (plain Java: {@code
 *       OutboxEngineBuilder.failureHandlerFor(type, handler)}; Spring starter: the map bean named
 *       {@code outboxPerTypeFailureHandlers}).
 *   <li>{@code FailureHandlers.defaults()} — the library-wide default chain ({@code Log →
 *       MaxRetries(10, DISABLE) → ExponentialBackoff(5s, 2x, cap 1h, jitter 20%)}).
 * </ol>
 *
 * <p>Listener callbacks for retry / disable / delete decisions ({@code onEventRetryScheduled},
 * {@code onEventDisabled}, {@code onEventDeleted}) are emitted by the engine dispatcher after the
 * decision is persisted to storage — {@code FailureHandler} implementations do not need to fire
 * them, and the built-in chain does not contain a listener-forwarding decorator.
 *
 * <p>Implementations MUST be thread-safe: the same handler instance can be invoked from multiple
 * worker threads concurrently.
 *
 * @param <T> payload type this handler reasons about
 */
@FunctionalInterface
public interface FailureHandler<T> {

    /**
     * Decide what should happen to the failed event. Called by the engine after {@code
     * EventHandler.handle(...)} returned a non-success outcome or threw an exception.
     */
    FailureDecision onFailure(FailureContext<T> ctx);
}
