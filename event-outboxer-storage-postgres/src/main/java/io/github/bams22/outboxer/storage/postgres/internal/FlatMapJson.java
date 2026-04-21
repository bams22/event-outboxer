/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.storage.postgres.internal;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Tiny JSON serialiser / parser for flat {@code Map<String, String>} values used by the outbox's
 * {@code trace_context} column. Keeps the PostgreSQL adapter free of a runtime JSON library
 * dependency: the {@code payload} column is handled by the user-configured
 * {@code EventSerializer}, but {@code trace_context} is always a flat string→string map.
 *
 * <p>Not intended for general-purpose JSON — only handles string values, rejects nested objects
 * and arrays on parse.
 */
public final class FlatMapJson {

  private FlatMapJson() {}

  public static String serialize(Map<String, String> map) {
    Objects.requireNonNull(map, "map must not be null");
    StringBuilder sb = new StringBuilder(map.size() * 16 + 2);
    sb.append('{');
    boolean first = true;
    for (Map.Entry<String, String> e : map.entrySet()) {
      if (!first) {
        sb.append(',');
      }
      sb.append('"').append(escape(e.getKey())).append("\":\"").append(escape(e.getValue())).append('"');
      first = false;
    }
    sb.append('}');
    return sb.toString();
  }

  public static Map<String, String> parse(String json) {
    Objects.requireNonNull(json, "json must not be null");
    String trimmed = json.trim();
    if (trimmed.isEmpty() || trimmed.equals("null")) {
      return Map.of();
    }
    if (trimmed.charAt(0) != '{' || trimmed.charAt(trimmed.length() - 1) != '}') {
      throw new IllegalArgumentException("expected a JSON object, got: " + trimmed);
    }
    Map<String, String> out = new LinkedHashMap<>();
    int i = 1;
    int end = trimmed.length() - 1;
    while (i < end) {
      i = skipWhitespace(trimmed, i);
      if (i >= end) {
        break;
      }
      if (trimmed.charAt(i) != '"') {
        throw new IllegalArgumentException("expected key at index " + i + ": " + trimmed);
      }
      StringBuilder key = new StringBuilder();
      i = readString(trimmed, i, key);
      i = skipWhitespace(trimmed, i);
      if (i >= end || trimmed.charAt(i) != ':') {
        throw new IllegalArgumentException("expected ':' after key at " + i + ": " + trimmed);
      }
      i++;
      i = skipWhitespace(trimmed, i);
      if (i >= end || trimmed.charAt(i) != '"') {
        throw new IllegalArgumentException(
            "only string values are supported in flat map JSON at " + i + ": " + trimmed);
      }
      StringBuilder value = new StringBuilder();
      i = readString(trimmed, i, value);
      out.put(key.toString(), value.toString());
      i = skipWhitespace(trimmed, i);
      if (i < end && trimmed.charAt(i) == ',') {
        i++;
      }
    }
    return out;
  }

  private static int skipWhitespace(String s, int i) {
    while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
      i++;
    }
    return i;
  }

  private static int readString(String s, int i, StringBuilder out) {
    if (s.charAt(i) != '"') {
      throw new IllegalArgumentException("expected '\"' at index " + i + ": " + s);
    }
    i++;
    while (i < s.length()) {
      char c = s.charAt(i);
      if (c == '"') {
        return i + 1;
      }
      if (c == '\\') {
        if (i + 1 >= s.length()) {
          throw new IllegalArgumentException("dangling escape at index " + i);
        }
        char next = s.charAt(i + 1);
        switch (next) {
          case '"' -> out.append('"');
          case '\\' -> out.append('\\');
          case '/' -> out.append('/');
          case 'n' -> out.append('\n');
          case 'r' -> out.append('\r');
          case 't' -> out.append('\t');
          case 'b' -> out.append('\b');
          case 'f' -> out.append('\f');
          case 'u' -> {
            if (i + 5 >= s.length()) {
              throw new IllegalArgumentException("truncated \\u escape at " + i);
            }
            int code = Integer.parseInt(s.substring(i + 2, i + 6), 16);
            out.append((char) code);
            i += 4;
          }
          default -> throw new IllegalArgumentException(
              "unsupported escape \\" + next + " at index " + i);
        }
        i += 2;
      } else {
        out.append(c);
        i++;
      }
    }
    throw new IllegalArgumentException("unterminated string starting at index " + i);
  }

  private static String escape(String s) {
    StringBuilder sb = new StringBuilder(s.length() + 2);
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"' -> sb.append("\\\"");
        case '\\' -> sb.append("\\\\");
        case '\n' -> sb.append("\\n");
        case '\r' -> sb.append("\\r");
        case '\t' -> sb.append("\\t");
        case '\b' -> sb.append("\\b");
        case '\f' -> sb.append("\\f");
        default -> {
          if (c < 0x20) {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
        }
      }
    }
    return sb.toString();
  }
}
