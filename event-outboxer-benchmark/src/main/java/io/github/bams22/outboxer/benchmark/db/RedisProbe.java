/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.db;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.Objects;

/**
 * Read-only questions to Redis for the report: how many commands the server processed (the locker's
 * load, sampled before and after) and how many lock keys are left. One short-lived connection per
 * probe.
 */
public final class RedisProbe implements AutoCloseable {

    private final RedisClient client;

    public RedisProbe(String uri) {
        this.client = RedisClient.create(Objects.requireNonNull(uri, "uri must not be null"));
    }

    /** {@code INFO server} version line. */
    public String serverVersion() {
        String info = withConnection(c -> c.info("server"));
        String version = field(info, "redis_version");
        String keydb = field(info, "keydb_version");
        return keydb.isEmpty() ? version : "KeyDB " + keydb + " (redis " + version + ")";
    }

    /**
     * {@code total_commands_processed} from {@code INFO stats}, cumulative since server start.
     * Includes the probe's own commands, a handful per run.
     */
    public long commandsProcessed() {
        String info = withConnection(c -> c.info("stats"));
        String value = field(info, "total_commands_processed");
        return value.isEmpty() ? 0 : Long.parseLong(value);
    }

    /** Number of keys matching {@code prefix*} via {@code SCAN}. */
    public long keysWithPrefix(String prefix) {
        return withConnection(
                c -> {
                    long count = 0;
                    KeyScanCursor<String> cursor =
                            c.scan(ScanArgs.Builder.matches(prefix + "*").limit(1000));
                    count += cursor.getKeys().size();
                    while (!cursor.isFinished()) {
                        cursor = c.scan(cursor, ScanArgs.Builder.matches(prefix + "*").limit(1000));
                        count += cursor.getKeys().size();
                    }
                    return count;
                });
    }

    @Override
    public void close() {
        client.shutdown();
    }

    private <T> T withConnection(java.util.function.Function<RedisCommands<String, String>, T> fn) {
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            return fn.apply(connection.sync());
        }
    }

    private static String field(String info, String name) {
        for (String line : info.split("\\r?\\n")) {
            if (line.startsWith(name + ":")) {
                return line.substring(name.length() + 1).trim();
            }
        }
        return "";
    }
}
