/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.domain;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.UUID;

/**
 * Opaque identifier for a worker JVM. One worker id is allocated per JVM process and shared across
 * all event types served from that JVM (see ADR-0005).
 *
 * <p>The default format is {@code {hostname}-{pid}-{uuid8}} (for example {@code
 * api-srv-01-4817-a3f2b1c9}). Custom identifiers may be supplied as long as they are unique across
 * the cluster and fit within 64 characters — the {@code event_outboxer.workers.worker_id} column in
 * the PostgreSQL adapter declares {@code VARCHAR(64)}.
 *
 * @param value non-blank, at most 64 characters
 */
public record WorkerId(String value) {

  /** Maximum length mirrors the PostgreSQL adapter's {@code VARCHAR(64)} column. */
  public static final int MAX_LENGTH = 64;

  public WorkerId {
    Objects.requireNonNull(value, "WorkerId value must not be null");
    if (value.isBlank()) {
      throw new IllegalArgumentException("WorkerId value must not be blank");
    }
    if (value.length() > MAX_LENGTH) {
      throw new IllegalArgumentException(
          "WorkerId value must be at most " + MAX_LENGTH + " characters, got " + value.length());
    }
  }

  /**
   * Generates a {@code WorkerId} in the default {@code {hostname}-{pid}-{uuid8}} form. The hostname
   * is truncated if necessary to keep the overall length within {@link #MAX_LENGTH}.
   */
  public static WorkerId generateDefault() {
    String host = hostname();
    long pid = ProcessHandle.current().pid();
    String shortUuid = UUID.randomUUID().toString().substring(0, 8);
    String candidate = host + "-" + pid + "-" + shortUuid;
    if (candidate.length() > MAX_LENGTH) {
      int overflow = candidate.length() - MAX_LENGTH;
      String truncatedHost = host.substring(0, Math.max(1, host.length() - overflow));
      candidate = truncatedHost + "-" + pid + "-" + shortUuid;
    }
    return new WorkerId(candidate);
  }

  private static String hostname() {
    try {
      String h = InetAddress.getLocalHost().getHostName();
      return h == null || h.isBlank() ? fallbackHostname() : h;
    } catch (UnknownHostException _) {
      return fallbackHostname();
    }
  }

  private static String fallbackHostname() {
    // JMX name is typically "pid@hostname"; if hostname resolution failed we may still have it.
    String name = ManagementFactory.getRuntimeMXBean().getName();
    int at = name.indexOf('@');
    if (at >= 0 && at + 1 < name.length()) {
      return name.substring(at + 1);
    }
    return "unknown-host";
  }
}
