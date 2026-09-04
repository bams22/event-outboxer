/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.lock.postgres.lease;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.jspecify.annotations.Nullable;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Receives lease release notifications for {@link PgLeaseEntityLocker}'s bounded wait (ADR-0035):
 * one dedicated session that {@code LISTEN}s on the schema's channel, on a daemon thread that
 * forwards every {@code NOTIFY} payload — the released lock key — to the locker's waiters.
 *
 * <p>Why one channel per schema and not one per key: a {@code NOTIFY} channel is an identifier of
 * at most 63 bytes, a lock key can be 512 characters. The release statement therefore sends {@code
 * pg_notify('<schema>.entity_locks', lock_key)} and the listener demultiplexes locally. The cost is
 * that every release in the fleet reaches every listening JVM — a few kilobytes per second at the
 * engine's throughput.
 *
 * <p>Connection discipline: {@code LISTEN} is session state, so the connection is borrowed from the
 * pool once and held for the listener's whole life — one pool slot, and a leak-detection warning if
 * the pool has one configured. Behind pgBouncer in transaction or statement pooling mode
 * notifications are never delivered; the listener therefore proves the path on every fresh session
 * by sending itself a probe from a second, briefly borrowed connection and waiting for it. A probe
 * that never arrives on the first session marks the listener {@link State#UNSUPPORTED} (warned
 * once, the locker keeps polling); a lost connection later on is reconnected with an exponential
 * back-off, and the waiters are woken on every reconnect so that a release missed in the gap costs
 * at most one probe.
 *
 * <p><b>Not usable behind pgBouncer in transaction or statement pooling mode.</b> {@code LISTEN} is
 * session state: it lands on whichever server connection served that one statement, the pooler
 * never forwards the notifications to this client, and worse, the server connection keeps the
 * subscription — the pooler may hand its notifications to whatever client it links next, where a
 * JDBC driver queues them unread. The listener therefore treats a probe that never arrives on the
 * first session as {@link State#UNSUPPORTED}, issues a best-effort {@code UNLISTEN *} on the same
 * JDBC connection before returning it (which reaches the right server connection only if the pooler
 * links the same one again), and the locker stops sending notifications. Leave the wake-up off
 * behind such a pooler, or give the listener a session-pooled or direct connection.
 *
 * <p>The session is opened and closed by the listener thread only: a pooled connection returned
 * while another thread still reads on it would be handed out to someone else. {@link #close()}
 * flips the state and interrupts the thread, which notices within one notification poll or at once
 * when parked in a reconnect back-off.
 */
public final class PgLeaseReleaseListener implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PgLeaseReleaseListener.class);

    /** Bounds how long {@link #close()} waits for the thread to notice. */
    private static final int NOTIFICATION_POLL_MILLIS = 1_000;

    private static final Duration PROBE_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration INITIAL_BACKOFF = Duration.ofMillis(200);
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(5);
    private static final String PROBE_PREFIX = "__event_outboxer_probe__:";

    /** Lifecycle of the listening session. */
    public enum State {
        /** Not connected yet, or reconnecting. */
        CONNECTING,
        /** Listening, and the probe round trip proved that notifications arrive. */
        ACTIVE,
        /** The first session never received its probe: no notifications on this path. */
        UNSUPPORTED,
        /** {@link #close()} was called. */
        CLOSED
    }

    private final DataSource dataSource;
    private final String channel;
    private final Consumer<String> onRelease;
    private final Runnable onReconnect;
    private final Thread thread;
    private final AtomicReference<State> state = new AtomicReference<>(State.CONNECTING);
    private volatile int backendPid = -1;

    /**
     * @param dataSource the application pool; one connection is held while the listener lives
     * @param channel the notification channel, {@link PgLeaseEntityLocker#channel()}
     * @param onRelease receives every notified lock key, on the listener thread
     * @param onReconnect called whenever a session was (re-)established or lost, so parked waiters
     *     re-probe instead of trusting a notification that may have been missed
     */
    public PgLeaseReleaseListener(
            DataSource dataSource,
            String channel,
            Consumer<String> onRelease,
            Runnable onReconnect) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.channel = Objects.requireNonNull(channel, "channel must not be null");
        this.onRelease = Objects.requireNonNull(onRelease, "onRelease must not be null");
        this.onReconnect = Objects.requireNonNull(onReconnect, "onReconnect must not be null");
        this.thread =
                Thread.ofPlatform()
                        .name("outbox-entity-locks-listen")
                        .daemon()
                        .unstarted(this::run);
    }

    /** Start the listening thread; connects and verifies in the background. */
    public void start() {
        thread.start();
    }

    public State state() {
        return state.get();
    }

    /** Whether waiters can rely on notifications right now. */
    public boolean isActive() {
        return state.get() == State.ACTIVE;
    }

    /**
     * Backend pid of the listening session, {@code -1} while not connected — tests and forensics.
     */
    public int backendPid() {
        return backendPid;
    }

    /** Stop listening; returns once the thread has closed its session or after five seconds. */
    @Override
    public void close() {
        state.set(State.CLOSED);
        thread.interrupt();
        try {
            thread.join(Duration.ofSeconds(5).toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void run() {
        long backoffNanos = INITIAL_BACKOFF.toNanos();
        boolean everActive = false;
        boolean warnedConnect = false;
        while (state.get() != State.CLOSED) {
            Connection conn = null;
            try {
                conn = dataSource.getConnection();
                conn.setAutoCommit(true);
                try (Statement st = conn.createStatement()) {
                    st.execute("LISTEN " + quoteIdentifier(channel));
                }
                PGConnection pg = conn.unwrap(PGConnection.class);
                backendPid = pg.getBackendPID();
                if (!probe(pg)) {
                    if (everActive) {
                        // Notifications worked on an earlier session: treat as transient.
                        throw new SQLException(
                                "probe notification did not arrive within " + PROBE_TIMEOUT);
                    }
                    if (!state.compareAndSet(State.CONNECTING, State.UNSUPPORTED)) {
                        return; // closed meanwhile
                    }
                    unlistenQuietly(conn);
                    log.warn(
                            "entity_locks release notifications are not delivered on this"
                                + " connection (LISTEN/NOTIFY needs a session-pinned connection;"
                                + " pgBouncer transaction pooling does not forward them) — the"
                                + " bounded lock wait polls instead. Set"
                                + " event-outboxer.lock.wakeup=false to silence this.");
                    return;
                }
                if (!state.compareAndSet(State.CONNECTING, State.ACTIVE)) {
                    return; // closed meanwhile
                }
                everActive = true;
                warnedConnect = false;
                backoffNanos = INITIAL_BACKOFF.toNanos();
                log.info("entity_locks release notifications active on channel '{}'", channel);
                onReconnect.run();
                while (state.get() == State.ACTIVE) {
                    forward(pg.getNotifications(NOTIFICATION_POLL_MILLIS));
                }
            } catch (Exception ex) {
                if (state.get() == State.CLOSED) {
                    return;
                }
                if (state.compareAndSet(State.ACTIVE, State.CONNECTING)) {
                    log.warn(
                            "entity_locks release listener lost its session, reconnecting: {}",
                            ex.toString());
                } else if (!warnedConnect) {
                    log.warn(
                            "entity_locks release listener could not connect, retrying with"
                                    + " back-off: {}",
                            ex.toString());
                    warnedConnect = true;
                }
                onReconnect.run();
                if (!sleepQuietly(backoffNanos)) {
                    return;
                }
                backoffNanos = Math.min(backoffNanos * 2, MAX_BACKOFF.toNanos());
            } finally {
                backendPid = -1;
                closeQuietly(conn);
            }
        }
    }

    /**
     * Sends a unique probe through a second pooled connection and waits for it on the listening
     * session, forwarding any real releases that arrive meanwhile.
     */
    private boolean probe(PGConnection pg) throws SQLException {
        String token = PROBE_PREFIX + UUID.randomUUID();
        try (Connection other = dataSource.getConnection();
                PreparedStatement ps = other.prepareStatement("SELECT pg_notify(?, ?)")) {
            other.setAutoCommit(true);
            ps.setString(1, channel);
            ps.setString(2, token);
            ps.execute();
        }
        long deadline = System.nanoTime() + PROBE_TIMEOUT.toNanos();
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return false;
            }
            PGNotification[] batch =
                    pg.getNotifications((int) Math.max(1, Duration.ofNanos(remaining).toMillis()));
            if (batch == null) {
                continue;
            }
            boolean seen = false;
            for (PGNotification n : batch) {
                if (token.equals(n.getParameter())) {
                    seen = true;
                } else {
                    forwardOne(n);
                }
            }
            if (seen) {
                return true;
            }
        }
    }

    private void forward(PGNotification @Nullable [] batch) {
        if (batch == null) {
            return;
        }
        for (PGNotification n : batch) {
            forwardOne(n);
        }
    }

    private void forwardOne(PGNotification n) {
        String key = n.getParameter();
        if (key == null || key.startsWith(PROBE_PREFIX)) {
            return;
        }
        try {
            onRelease.accept(key);
        } catch (RuntimeException ex) {
            log.debug("release callback failed for key '{}': {}", key, ex.toString());
        }
    }

    private boolean sleepQuietly(long nanos) {
        try {
            Thread.sleep(Duration.ofNanos(nanos));
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Best-effort cleanup of a subscription that may have landed on a pooled server connection
     * behind a transaction-mode pooler; on a direct connection it is merely redundant.
     */
    private static void unlistenQuietly(Connection conn) {
        try (Statement st = conn.createStatement()) {
            st.execute("UNLISTEN *");
        } catch (SQLException ex) {
            log.debug("UNLISTEN after an unsupported probe failed: {}", ex.toString());
        }
    }

    private static void closeQuietly(@Nullable Connection conn) {
        if (conn == null) {
            return;
        }
        try {
            conn.close();
        } catch (SQLException ex) {
            log.debug("closing the listener session failed: {}", ex.toString());
        }
    }

    /** Double-quoted identifier, embedded quotes doubled. */
    static String quoteIdentifier(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }
}
