/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark;

import io.github.bams22.outboxer.api.publish.OutboxEventPublisher;
import io.github.bams22.outboxer.benchmark.db.DatabaseHandle;
import io.github.bams22.outboxer.benchmark.db.PgProbe;
import io.github.bams22.outboxer.benchmark.db.RedisHandle;
import io.github.bams22.outboxer.benchmark.db.RedisProbe;
import io.github.bams22.outboxer.benchmark.db.StorageState;
import io.github.bams22.outboxer.benchmark.db.TableWrites;
import io.github.bams22.outboxer.benchmark.ledger.Handling;
import io.github.bams22.outboxer.benchmark.ledger.InMemoryLedger;
import io.github.bams22.outboxer.benchmark.ledger.JdbcLedger;
import io.github.bams22.outboxer.benchmark.ledger.Ledger;
import io.github.bams22.outboxer.benchmark.report.BenchmarkReport;
import io.github.bams22.outboxer.benchmark.report.LatencyStats;
import io.github.bams22.outboxer.benchmark.scenario.Chaos;
import io.github.bams22.outboxer.benchmark.scenario.FleetMode;
import io.github.bams22.outboxer.benchmark.scenario.LockType;
import io.github.bams22.outboxer.benchmark.scenario.PostgresRestart;
import io.github.bams22.outboxer.benchmark.scenario.Scenario;
import io.github.bams22.outboxer.benchmark.target.BenchmarkEnvironment;
import io.github.bams22.outboxer.benchmark.target.BenchmarkEvent;
import io.github.bams22.outboxer.benchmark.target.BenchmarkPublisher;
import io.github.bams22.outboxer.benchmark.target.BenchmarkTarget;
import io.github.bams22.outboxer.benchmark.target.TargetSession;
import io.github.bams22.outboxer.benchmark.verify.ChaosEvent;
import io.github.bams22.outboxer.benchmark.verify.InvariantChecker;
import io.github.bams22.outboxer.benchmark.verify.InvariantReport;
import java.net.InetAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One run, end to end: database up, target open, publish, drain (with chaos), close, sample, grade,
 * report. The sequence is the same for every target; only the session differs.
 *
 * <p>Phase boundaries: the closing {@code pg_stat} sample, the ledger snapshot and the
 * storage-cleanliness check all run <em>after</em> the session is closed. Closing drops every
 * connection, and a PostgreSQL backend flushes its pending statistics on exit — while it lives,
 * idle backends may hold counters back for up to ten seconds, which would understate the database
 * cost. Grading after the graceful stop also means a fleet that abandons claims on shutdown fails
 * the run instead of hiding behind in-flight rows.
 *
 * <p>Chaos actions fire from the drain loop, each once, when handled progress reaches its trigger.
 * They are recorded as {@link ChaosEvent}s that the checker uses to attribute duplicates and the
 * storage check uses to discount leases nobody alive could release.
 */
public final class BenchmarkRun {

    private static final Logger log = LoggerFactory.getLogger(BenchmarkRun.class);
    private static final Duration STATS_SETTLE = Duration.ofMillis(500);
    private static final Duration DRAIN_POLL = Duration.ofMillis(100);
    private static final DateTimeFormatter WORK_STAMP =
            DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneOffset.UTC);

    private final BenchmarkOptions options;
    private final BenchmarkTarget target;

    public BenchmarkRun(BenchmarkOptions options, BenchmarkTarget target) {
        this.options = Objects.requireNonNull(options, "options must not be null");
        this.target = Objects.requireNonNull(target, "target must not be null");
    }

    /**
     * Executes the run and returns its report; never throws for a failed invariant, only for a
     * broken environment or an impossible configuration.
     */
    public BenchmarkReport run() throws InterruptedException {
        Scenario scenario = options.scenario();
        Instant runStart = Instant.now();
        Path workDir =
                options.reportDir()
                        .resolve("work")
                        .resolve(scenario.name() + "-" + WORK_STAMP.format(runStart));
        try (DatabaseHandle db = openDatabase()) {
            if (scenario.chaos().postgresRestart() != PostgresRestart.NONE
                    && !db.supportsRestart()) {
                throw new IllegalArgumentException(
                        "--bench.pg-restart needs the disposable Testcontainers database; drop"
                                + " --bench.jdbc-url or set --bench.pg-restart=none");
            }
            PgProbe probe = new PgProbe(db.coordinates());
            String postgresVersion = probe.serverVersion();
            String schema = target.storageSchema();
            try (RedisSide redis = openRedis(scenario);
                    LedgerHandle ledgerHandle = openLedger(db, scenario)) {
                Ledger ledger = ledgerHandle.ledger;
                BenchmarkEnvironment env =
                        new BenchmarkEnvironment(
                                db.coordinates(), redis.uri(), scenario, ledger, workDir);

                log.info(
                        "Opening target {} against {} ({} fleet)",
                        target.name(),
                        db.origin(),
                        scenario.fleet().option());
                PublishPhase publish;
                Drain drain;
                TableWrites before;
                long redisBefore = 0;
                List<ChaosEvent> chaos = new ArrayList<>();
                try (TargetSession session = target.open(env)) {
                    if (!scenario.workersStartAfterPublish()) {
                        session.startWorkers();
                    }
                    before = probe.tableWrites(schema);
                    redisBefore = redis.commandsProcessed();
                    Instant phaseStart = Instant.now();
                    publish = publishAll(session.publisher(), scenario);
                    if (scenario.workersStartAfterPublish()) {
                        phaseStart = Instant.now();
                        session.startWorkers();
                    }
                    drain = awaitDrain(ledger, scenario, phaseStart, session, db, chaos);
                    log.info("Stopping target");
                }
                Thread.sleep(STATS_SETTLE);
                TableWrites writes = probe.tableWrites(schema).minus(before);
                BenchmarkReport.@Nullable RedisMetrics redisMetrics =
                        redis.metrics(redisBefore, scenario.events());
                long totalHandlings = ledger.total();
                List<Handling> handlings = ledger.snapshot();
                StorageState storage =
                        probe.storageState(schema, killedWorkers(chaos), lastRestart(chaos));

                InvariantReport invariants =
                        new InvariantChecker()
                                .check(
                                        scenario.events(),
                                        handlings,
                                        scenario.lockType().exclusive(),
                                        chaos);
                ProcessingSummary processing = summarize(publish, handlings, drain, scenario);

                return BenchmarkReport.builder()
                        .target(target.name())
                        .scenario(scenario)
                        .environment(
                                environment(
                                        db.origin(),
                                        postgresVersion,
                                        redis.origin(),
                                        redis.serverVersion()))
                        .startedAt(runStart)
                        .finishedAt(Instant.now())
                        .publish(
                                new BenchmarkReport.PublishMetrics(
                                        scenario.events(),
                                        publish.duration,
                                        rate(scenario.events(), publish.duration),
                                        LatencyStats.of(publish.latencyNanos)))
                        .processing(
                                new BenchmarkReport.ProcessingMetrics(
                                        drain.drained,
                                        invariants.succeeded(),
                                        totalHandlings,
                                        processing.duration,
                                        rate(invariants.succeeded(), processing.duration),
                                        processing.endToEnd,
                                        invariants.retries()))
                        .database(
                                new BenchmarkReport.DatabaseMetrics(
                                        writes,
                                        (double) writes.total() / scenario.events(),
                                        databaseCaveat(scenario, chaos)))
                        .redis(redisMetrics)
                        .invariants(invariants)
                        .storage(storage)
                        .chaos(chaos)
                        .build();
            }
        }
    }

    private DatabaseHandle openDatabase() {
        return options.database() != null
                ? DatabaseHandle.external(options.database())
                : DatabaseHandle.testcontainers(options.postgresImage());
    }

    private RedisSide openRedis(Scenario scenario) {
        if (scenario.lockType() != LockType.REDIS) {
            return RedisSide.NONE;
        }
        RedisHandle handle =
                options.redisUri() != null
                        ? RedisHandle.external(options.redisUri())
                        : RedisHandle.testcontainers(options.redisImage());
        return new RedisSide(handle, new RedisProbe(handle.uri()));
    }

    private static LedgerHandle openLedger(DatabaseHandle db, Scenario scenario) {
        if (scenario.fleet() == FleetMode.FORKED) {
            JdbcLedger jdbc = new JdbcLedger(db.coordinates(), 2, "ledger-driver");
            jdbc.install();
            return new LedgerHandle(jdbc, jdbc);
        }
        return new LedgerHandle(new InMemoryLedger(), null);
    }

    private static PublishPhase publishAll(BenchmarkPublisher publisher, Scenario s)
            throws InterruptedException {
        int n = s.events();
        long[] publishedAtMicros = new long[n];
        long[] latencyNanos = new long[n];
        String padding = "x".repeat(s.payloadBytes());
        AtomicLong next = new AtomicLong();
        ExecutorService pool = Executors.newFixedThreadPool(s.publisherThreads());
        Instant start = Instant.now();
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < s.publisherThreads(); t++) {
                futures.add(
                        pool.submit(
                                () -> {
                                    long seq;
                                    while ((seq = next.getAndIncrement()) < n) {
                                        BenchmarkEvent event =
                                                new BenchmarkEvent(
                                                        seq,
                                                        s.typeIndexFor(seq),
                                                        s.lockKeyFor(seq),
                                                        padding);
                                        Instant before = Instant.now();
                                        long t0 = System.nanoTime();
                                        publisher.publish(event);
                                        latencyNanos[(int) seq] = System.nanoTime() - t0;
                                        publishedAtMicros[(int) seq] =
                                                LatencyStats.epochMicros(before);
                                    }
                                }));
            }
            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (java.util.concurrent.ExecutionException e) {
                    throw new IllegalStateException("Publishing failed", e.getCause());
                }
            }
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(10, TimeUnit.SECONDS);
        }
        return new PublishPhase(
                Duration.between(start, Instant.now()), publishedAtMicros, latencyNanos);
    }

    /**
     * Polls the ledger until every event has a successful handling or the deadline passes, firing
     * chaos actions on the way. A ledger read that fails (database down under chaos) is retried on
     * the next tick.
     */
    private static Drain awaitDrain(
            Ledger ledger,
            Scenario s,
            Instant phaseStart,
            TargetSession session,
            DatabaseHandle db,
            List<ChaosEvent> chaos)
            throws InterruptedException {
        Chaos plan = s.chaos();
        boolean killPending = plan.killWorkers() > 0;
        boolean restartPending = plan.postgresRestart() != PostgresRestart.NONE;
        Instant deadline = Instant.now().plus(s.drainTimeout());
        long lastLogged = -1;
        while (true) {
            long done;
            try {
                done = ledger.distinctSuccesses();
            } catch (RuntimeException e) {
                log.debug("Ledger unavailable, retrying: {}", e.getMessage());
                done = -1;
            }
            if (done >= s.events()) {
                return new Drain(true, phaseStart, Duration.between(phaseStart, Instant.now()));
            }
            if (Instant.now().isAfter(deadline)) {
                log.warn("Drain timed out: {}/{} events handled", done, s.events());
                return new Drain(false, phaseStart, Duration.between(phaseStart, Instant.now()));
            }
            if (done >= 0) {
                if (killPending && done >= plan.killAtProgress() * s.events()) {
                    killPending = false;
                    killAndMaybeRespawn(session, plan, done, chaos);
                }
                if (restartPending && done >= plan.postgresRestartAtProgress() * s.events()) {
                    restartPending = false;
                    restartDatabase(db, plan.postgresRestart(), done, chaos);
                }
                if (done / 1000 != lastLogged / 1000) {
                    log.info("Handled {}/{}", done, s.events());
                    lastLogged = done;
                }
            }
            Thread.sleep(DRAIN_POLL);
        }
    }

    private static void killAndMaybeRespawn(
            TargetSession session, Chaos plan, long progress, List<ChaosEvent> chaos) {
        Instant at = Instant.now();
        List<String> killed = session.killWorkers(plan.killWorkers());
        chaos.add(new ChaosEvent(ChaosEvent.Kind.WORKER_KILLED, at, progress, killed, "SIGKILL"));
        log.info("Killed {} at {} handled", killed, progress);
        if (plan.respawnKilled()) {
            Instant spawnAt = Instant.now();
            List<String> spawned = session.spawnWorkers(killed.size());
            chaos.add(
                    new ChaosEvent(
                            ChaosEvent.Kind.WORKER_SPAWNED,
                            spawnAt,
                            progress,
                            spawned,
                            "ready after "
                                    + Duration.between(spawnAt, Instant.now()).toMillis()
                                    + "ms"));
            log.info("Respawned {}", spawned);
        }
    }

    private static void restartDatabase(
            DatabaseHandle db, PostgresRestart mode, long progress, List<ChaosEvent> chaos) {
        Instant at = Instant.now();
        Duration outage = db.restart(mode);
        chaos.add(
                new ChaosEvent(
                        ChaosEvent.Kind.POSTGRES_RESTARTED,
                        at,
                        progress,
                        List.of(),
                        mode.option() + " restart, unreachable for " + outage.toMillis() + "ms"));
        log.info(
                "PostgreSQL {} restart at {} handled, back after {}",
                mode.option(),
                progress,
                outage);
    }

    private static @Nullable String databaseCaveat(Scenario scenario, List<ChaosEvent> chaos) {
        boolean crashed =
                scenario.chaos().postgresRestart() == PostgresRestart.CRASH
                        && lastRestart(chaos) != null;
        return crashed
                ? "unreliable: the crash restart reset pg_stat counters, so writes cover only the"
                        + " part of the run after it"
                : null;
    }

    private static List<String> killedWorkers(List<ChaosEvent> chaos) {
        List<String> ids = new ArrayList<>();
        for (ChaosEvent c : chaos) {
            if (c.kind() == ChaosEvent.Kind.WORKER_KILLED) {
                ids.addAll(c.workerIds());
            }
        }
        return ids;
    }

    private static @Nullable Instant lastRestart(List<ChaosEvent> chaos) {
        Instant last = null;
        for (ChaosEvent c : chaos) {
            if (c.kind() == ChaosEvent.Kind.POSTGRES_RESTARTED) {
                last = c.at();
            }
        }
        return last;
    }

    private static ProcessingSummary summarize(
            PublishPhase publish, List<Handling> handlings, Drain drain, Scenario s) {
        long[] firstSuccessMicros = new long[s.events()];
        java.util.Arrays.fill(firstSuccessMicros, Long.MAX_VALUE);
        long lastFinish = Long.MIN_VALUE;
        for (Handling h : handlings) {
            if (!h.succeeded() || h.seq() < 0 || h.seq() >= s.events()) {
                continue;
            }
            long finished = LatencyStats.epochMicros(h.finishedAt());
            int i = (int) h.seq();
            firstSuccessMicros[i] = Math.min(firstSuccessMicros[i], finished);
            lastFinish = Math.max(lastFinish, finished);
        }
        long[] e2eNanos = new long[s.events()];
        int count = 0;
        for (int i = 0; i < s.events(); i++) {
            if (firstSuccessMicros[i] != Long.MAX_VALUE) {
                long micros = firstSuccessMicros[i] - publish.publishedAtMicros[i];
                e2eNanos[count++] = Math.max(0, micros) * 1_000L;
            }
        }
        // Prefer the ledger's own last finish over the poll loop's wall time: it is exact, and the
        // poll loop adds up to one DRAIN_POLL. Fall back to the wall time on a timed-out drain.
        Duration duration = drain.wallDuration;
        if (lastFinish != Long.MIN_VALUE && drain.drained) {
            long phaseStartMicros = LatencyStats.epochMicros(drain.phaseStart);
            duration = Duration.ofNanos(Math.max(1, lastFinish - phaseStartMicros) * 1_000L);
        }
        return new ProcessingSummary(duration, LatencyStats.of(e2eNanos, count));
    }

    private static double rate(long count, Duration d) {
        double seconds = d.toNanos() / 1_000_000_000.0;
        return seconds <= 0 ? 0 : count / seconds;
    }

    private static BenchmarkReport.Environment environment(
            String origin,
            String pgVersion,
            @Nullable String redisOrigin,
            @Nullable String redisVersion) {
        Runtime rt = Runtime.getRuntime();
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (java.net.UnknownHostException e) {
            host = "unknown";
        }
        String library = OutboxEventPublisher.class.getPackage().getImplementationVersion();
        return BenchmarkReport.Environment.builder()
                .javaVersion(System.getProperty("java.version"))
                .os(
                        System.getProperty("os.name")
                                + " "
                                + System.getProperty("os.version")
                                + " ("
                                + System.getProperty("os.arch")
                                + ")")
                .availableProcessors(rt.availableProcessors())
                .maxHeapMb(rt.maxMemory() / (1024 * 1024))
                .host(host)
                .databaseOrigin(origin)
                .postgresVersion(pgVersion)
                .redisOrigin(redisOrigin)
                .redisVersion(redisVersion)
                .libraryVersion(library == null ? "working-tree" : library)
                .build();
    }

    private record PublishPhase(Duration duration, long[] publishedAtMicros, long[] latencyNanos) {}

    private record Drain(boolean drained, Instant phaseStart, Duration wallDuration) {}

    private record ProcessingSummary(Duration duration, LatencyStats endToEnd) {}

    /**
     * The Redis the redis locker uses, plus its probe; {@link #NONE} when the scenario has no
     * Redis, so the run code needs no null checks.
     */
    private record RedisSide(@Nullable RedisHandle handle, @Nullable RedisProbe probe)
            implements AutoCloseable {

        static final RedisSide NONE = new RedisSide(null, null);

        private static final String LOCK_PREFIX = "outbox:lock:";

        @Nullable String uri() {
            return handle == null ? null : handle.uri();
        }

        @Nullable String origin() {
            return handle == null ? null : handle.origin();
        }

        @Nullable String serverVersion() {
            return probe == null ? null : probe.serverVersion();
        }

        long commandsProcessed() {
            return probe == null ? 0 : probe.commandsProcessed();
        }

        BenchmarkReport.@Nullable RedisMetrics metrics(long before, long events) {
            if (probe == null) {
                return null;
            }
            long commands = probe.commandsProcessed() - before;
            return new BenchmarkReport.RedisMetrics(
                    commands, (double) commands / events, probe.keysWithPrefix(LOCK_PREFIX));
        }

        @Override
        public void close() {
            if (probe != null) {
                probe.close();
            }
            if (handle != null) {
                handle.close();
            }
        }
    }

    /** The ledger plus whatever must be closed with it (the JDBC pool for the forked fleet). */
    private record LedgerHandle(Ledger ledger, @Nullable AutoCloseable resource)
            implements AutoCloseable {
        @Override
        public void close() {
            if (resource != null) {
                try {
                    resource.close();
                } catch (Exception e) {
                    log.warn("Ledger close failed: {}", e.getMessage());
                }
            }
        }
    }
}
