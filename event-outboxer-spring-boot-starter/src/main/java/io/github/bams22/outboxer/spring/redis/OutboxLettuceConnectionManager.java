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

import io.github.bams22.outboxer.spring.OutboxProperties;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.StringUtils;

/**
 * Single lifecycle owner of the starter-managed Lettuce client and connection (ADR-0027): builds
 * the {@link RedisURI} from {@code event-outboxer.redis.*}, connects eagerly at startup, and on
 * context shutdown closes the connection before shutting the client down. The exposed connection
 * bean deliberately suppresses Spring's inferred {@code close()} so this class stays the only owner
 * — no double-close, correct ordering.
 */
final class OutboxLettuceConnectionManager implements DisposableBean {

    private final RedisClient client;
    private final StatefulRedisConnection<String, String> connection;

    OutboxLettuceConnectionManager(OutboxProperties.Redis properties) {
        RedisURI uri = buildUri(properties);
        this.client = RedisClient.create(uri);
        try {
            this.connection = client.connect();
        } catch (RuntimeException ex) {
            client.shutdown(Duration.ZERO, Duration.ofSeconds(2));
            throw new IllegalStateException(
                    "Could not connect to Redis at "
                            + uri
                            + " for the outbox (configured via event-outboxer.redis.*). Check the"
                            + " connection properties, or define your own"
                            + " StatefulRedisConnection<String, String> bean instead.",
                    ex);
        }
    }

    StatefulRedisConnection<String, String> getConnection() {
        return connection;
    }

    @Override
    public void destroy() {
        connection.close();
        client.shutdown(Duration.ZERO, Duration.ofSeconds(2));
    }

    private static RedisURI buildUri(OutboxProperties.Redis properties) {
        if (StringUtils.hasText(properties.getUri())) {
            return applyTimeout(RedisURI.create(properties.getUri()), properties);
        }
        String host = properties.getHost();
        if (!StringUtils.hasText(host)) {
            // Unreachable behind OnOutboxRedisConfiguredCondition.
            throw new IllegalStateException(
                    "Set event-outboxer.redis.uri or event-outboxer.redis.host.");
        }
        RedisURI.Builder builder =
                RedisURI.Builder.redis(host, properties.getPort())
                        .withDatabase(properties.getDatabase())
                        .withSsl(properties.isSsl());
        String password = properties.getPassword();
        if (StringUtils.hasText(properties.getUsername()) && password != null) {
            builder.withAuthentication(properties.getUsername(), password.toCharArray());
        } else if (password != null) {
            builder.withPassword(password.toCharArray());
        }
        if (StringUtils.hasText(properties.getClientName())) {
            builder.withClientName(properties.getClientName());
        }
        return applyTimeout(builder.build(), properties);
    }

    private static RedisURI applyTimeout(RedisURI uri, OutboxProperties.Redis properties) {
        if (properties.getTimeout() != null) {
            uri.setTimeout(properties.getTimeout());
        }
        return uri;
    }
}
