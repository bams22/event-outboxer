/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.redis;

import io.github.bams22.outboxer.spring.OutboxRedisConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import java.util.Collection;
import java.util.List;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider;

/**
 * Resolves the {@link StatefulRedisConnection} the outbox uses, shared by the Redis lock and Redis
 * cache auto-configurations (ADR-0027): {@link OutboxRedisConnection @OutboxRedisConnection}-
 * qualified bean first, then Spring's unique/{@code @Primary} bean, then a descriptive failure.
 *
 * <p>Both lookups go through {@link ObjectProvider} so nothing is injected eagerly — with several
 * unmarked connection beans a qualified one still resolves cleanly instead of tripping Spring's
 * stock {@code NoUniqueBeanDefinitionException} on a fallback parameter.
 */
public final class OutboxRedisConnectionResolver {

    private OutboxRedisConnectionResolver() {}

    /**
     * Strict resolution for wiring that cannot proceed without a connection: the
     * {@code @OutboxRedisConnection}-qualified bean, else the unique or {@code @Primary} bean, else
     * {@link AmbiguousOutboxRedisConnectionException} naming the candidates — or, with no candidate
     * at all, pointing at {@code event-outboxer.redis.*} (there is no silent back-off: a
     * Redis-backed feature was explicitly enabled).
     *
     * @param qualified provider injected with {@code @OutboxRedisConnection
     *     ObjectProvider<StatefulRedisConnection<String, String>>}
     * @param all provider injected without a qualifier (all connection beans)
     * @param beanFactory used only to name the candidates in the failure message
     */
    public static StatefulRedisConnection<String, String> resolve(
            ObjectProvider<StatefulRedisConnection<String, String>> qualified,
            ObjectProvider<StatefulRedisConnection<String, String>> all,
            ListableBeanFactory beanFactory) {
        StatefulRedisConnection<String, String> qualifiedBean;
        try {
            qualifiedBean = qualified.getIfAvailable();
        } catch (NoUniqueBeanDefinitionException ex) {
            // A pub/sub connection is a StatefulRedisConnection too, but never a command
            // connection for the outbox: ignore it when that alone resolves the ambiguity.
            StatefulRedisConnection<String, String> commandOnly = soleCommandConnection(qualified);
            if (commandOnly != null) {
                return commandOnly;
            }
            throw AmbiguousOutboxRedisConnectionException.multipleQualified(namesFrom(ex));
        }
        if (qualifiedBean != null) {
            return qualifiedBean;
        }
        StatefulRedisConnection<String, String> unique = all.getIfUnique();
        if (unique != null) {
            return unique;
        }
        StatefulRedisConnection<String, String> commandOnly = soleCommandConnection(all);
        if (commandOnly != null) {
            return commandOnly;
        }
        List<String> candidates =
                List.of(
                        beanFactory.getBeanNamesForType(
                                StatefulRedisConnection.class, true, false));
        if (candidates.isEmpty()) {
            throw AmbiguousOutboxRedisConnectionException.noneAvailable();
        }
        throw AmbiguousOutboxRedisConnectionException.noneQualified(candidates);
    }

    /**
     * The pub/sub connection for lock release notifications (ADR-0035 wake-up): the
     * {@code @OutboxRedisConnection}-qualified {@code StatefulRedisPubSubConnection} bean, else the
     * unique one, else {@code null} — the wake-up is an optimisation, so nothing fails without it.
     *
     * @param qualified provider injected with {@code @OutboxRedisConnection
     *     ObjectProvider<StatefulRedisPubSubConnection<String, String>>}
     * @param all provider injected without a qualifier
     */
    public static @Nullable StatefulRedisPubSubConnection<String, String> resolvePubSub(
            ObjectProvider<StatefulRedisPubSubConnection<String, String>> qualified,
            ObjectProvider<StatefulRedisPubSubConnection<String, String>> all) {
        StatefulRedisPubSubConnection<String, String> qualifiedBean;
        try {
            qualifiedBean = qualified.getIfAvailable();
        } catch (NoUniqueBeanDefinitionException ex) {
            throw AmbiguousOutboxRedisConnectionException.multipleQualified(namesFrom(ex));
        }
        if (qualifiedBean != null) {
            return qualifiedBean;
        }
        return all.getIfUnique();
    }

    private static @Nullable StatefulRedisConnection<String, String> soleCommandConnection(
            ObjectProvider<StatefulRedisConnection<String, String>> provider) {
        List<StatefulRedisConnection<String, String>> commandOnly =
                provider.stream()
                        .filter(c -> !(c instanceof StatefulRedisPubSubConnection))
                        .toList();
        return commandOnly.size() == 1 ? commandOnly.get(0) : null;
    }

    private static Collection<String> namesFrom(NoUniqueBeanDefinitionException ex) {
        Collection<String> names = ex.getBeanNamesFound();
        return names != null ? names : List.of();
    }
}
