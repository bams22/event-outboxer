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

import io.github.bams22.outboxer.benchmark.BenchmarkRunner;
import io.github.bams22.outboxer.benchmark.target.BenchmarkEnvironment;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Phase-2 fleet: one JVM per worker, forked from this JVM with the same code — the exec jar when
 * running from it, the current class path otherwise — and told what to do through a {@link
 * WorkerSpec} file. Each worker writes its output to {@code <workDir>/<id>.log} and creates {@code
 * <workDir>/<id>.ready} once its context is up.
 *
 * <p>{@link #kill} is {@code SIGKILL}; {@link #close} is {@code SIGTERM} followed by a bounded wait
 * for the graceful stop, then {@code SIGKILL} for stragglers.
 */
final class ForkedFleet implements WorkerFleet {

    private static final Logger log = LoggerFactory.getLogger(ForkedFleet.class);
    private static final Duration READY_TIMEOUT = Duration.ofSeconds(90);
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(45);
    private static final String BOOT_LAUNCHER =
            "org.springframework.boot.loader.launch.JarLauncher";

    private final BenchmarkEnvironment env;
    private final Map<String, Process> workers = new LinkedHashMap<>();
    private int next;

    ForkedFleet(BenchmarkEnvironment env) {
        this.env = env;
        try {
            Files.createDirectories(env.workDir());
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot create work dir " + env.workDir(), e);
        }
    }

    @Override
    public synchronized List<String> start(int count) {
        List<String> ids = new ArrayList<>();
        Map<String, Path> readyFiles = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            String id = OutboxerTarget.WORKER_ID_PREFIX + next++;
            Path spec = env.workDir().resolve(id + ".json");
            Path ready = env.workDir().resolve(id + ".ready");
            Path logFile = env.workDir().resolve(id + ".log");
            new WorkerSpec(env.database(), env.redisUri(), env.scenario(), id, ready.toString())
                    .write(spec);
            workers.put(id, launch(spec, logFile));
            readyFiles.put(id, ready);
            ids.add(id);
        }
        awaitReady(readyFiles);
        return ids;
    }

    @Override
    public synchronized List<String> ids() {
        return List.copyOf(workers.keySet());
    }

    @Override
    public synchronized List<String> kill(int count) {
        List<String> killed = new ArrayList<>();
        for (String id : new ArrayList<>(workers.keySet())) {
            if (killed.size() == count) {
                break;
            }
            Process p = workers.remove(id);
            p.destroyForcibly();
            try {
                p.waitFor(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            log.info("SIGKILL {} (pid {})", id, p.pid());
            killed.add(id);
        }
        return killed;
    }

    @Override
    public synchronized void close() {
        workers.values().forEach(Process::destroy);
        Instant deadline = Instant.now().plus(STOP_TIMEOUT);
        for (Map.Entry<String, Process> e : workers.entrySet()) {
            Process p = e.getValue();
            try {
                long left = Math.max(1, Duration.between(Instant.now(), deadline).toMillis());
                if (!p.waitFor(left, TimeUnit.MILLISECONDS)) {
                    log.warn("{} did not stop within {}; killing", e.getKey(), STOP_TIMEOUT);
                    p.destroyForcibly();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                p.destroyForcibly();
            }
        }
        workers.clear();
    }

    private Process launch(Path spec, Path logFile) {
        List<String> command = new ArrayList<>();
        command.add(javaBinary());
        command.addAll(env.scenario().workerJvmArgs());
        command.add("-cp");
        if (runningFromBootJar()) {
            command.add(System.getProperty("java.class.path"));
            command.add(BOOT_LAUNCHER);
        } else {
            command.add(System.getProperty("java.class.path"));
            command.add(BenchmarkRunner.class.getName());
        }
        command.add(BenchmarkRunner.ROLE_ARG + "worker");
        command.add(BenchmarkRunner.SPEC_ARG + spec);
        try {
            return new ProcessBuilder(command)
                    .redirectErrorStream(true)
                    .redirectOutput(logFile.toFile())
                    .start();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot fork worker: " + command, e);
        }
    }

    private void awaitReady(Map<String, Path> readyFiles) {
        Instant deadline = Instant.now().plus(READY_TIMEOUT);
        Map<String, Path> pending = new LinkedHashMap<>(readyFiles);
        while (!pending.isEmpty()) {
            pending.entrySet().removeIf(e -> Files.exists(e.getValue()));
            for (String id : pending.keySet()) {
                Process p = workers.get(id);
                if (p != null && !p.isAlive()) {
                    throw new IllegalStateException(
                            "Worker "
                                    + id
                                    + " exited with code "
                                    + p.exitValue()
                                    + " before becoming ready; see "
                                    + env.workDir().resolve(id + ".log"));
                }
            }
            if (pending.isEmpty()) {
                break;
            }
            if (Instant.now().isAfter(deadline)) {
                throw new IllegalStateException(
                        "Workers not ready within " + READY_TIMEOUT + ": " + pending.keySet());
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for workers", e);
            }
        }
    }

    private static String javaBinary() {
        return ProcessHandle.current()
                .info()
                .command()
                .orElseGet(
                        () -> Path.of(System.getProperty("java.home"), "bin", "java").toString());
    }

    private static boolean runningFromBootJar() {
        var source = ForkedFleet.class.getProtectionDomain().getCodeSource();
        return source != null && source.getLocation().toString().contains("BOOT-INF");
    }
}
