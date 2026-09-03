# event-outboxer-admin-actuator

An Actuator endpoint (`outboxadmin`) over the `OutboxAdmin` SPI
([ADR-0019](../adr/0019-admin-and-retention-surface.md)): inspect
`DISABLED` / archived events, re-enable them after a fix, replay
archived events for re-execution
([ADR-0033](../adr/0033-archive-dedup-key-and-replay-from-archive.md)),
purge old rows — all through the management port with the standard
Actuator exposure and security model.

| | |
|---|---|
| Coordinates | `io.github.bams22:event-outboxer-admin-actuator` |
| Java package | `io.github.bams22.outboxer.admin.actuator` |
| Depends on | [`event-outboxer-api`](event-outboxer-api.md), [`event-outboxer-spi`](event-outboxer-spi.md), Spring Boot Actuator |
| Endpoint id | `outboxadmin` (`outbox` is taken by the health indicator) |
| Enable with | module on the classpath + `management.endpoints.web.exposure.include=outboxadmin` |

## Why it exists

Events end up `DISABLED` (retries exhausted, poison payloads) and, with
the archive enabled, successful events accumulate in
`event_outboxer.event_archive`. Operators need to look at them,
re-enable them after deploying a fix, and clean them up — without
hand-written SQL against library tables. This module is the
management-port surface for that; a functionally equivalent
application-port surface exists as
[`event-outboxer-admin-rest`](event-outboxer-admin-rest.md) for setups
where Actuator is not exposed.

## What it does

`OutboxAdminEndpoint` maps `OutboxAdmin` operations onto Actuator
read / write / delete operations:

| HTTP | Operation |
|---|---|
| `GET /actuator/outboxadmin?status=DISABLED&eventType=X&limit=50&cursor=…` | list events, keyset-paginated newest-first; all params optional (defaults `status=DISABLED`, `limit=50`); response = `events` + `nextCursor` |
| `GET /actuator/outboxadmin/{id}` | one event — active store first, then the archive; 404 when absent |
| `POST /actuator/outboxadmin/{id}` (empty body) | re-enable one `DISABLED` event → `PENDING` with a **fresh attempts budget**; response `{"reenabled": true/false}`. With body `{"action": "replay"}`: replay one archived event (ADR-0033); response `{"outcome": "REPLAYED"/"COALESCED"/"ID_IN_USE"/"NOT_FOUND"}` — `ID_IN_USE` means the hot table already holds that id (the app re-published the UUID), so the archive row is kept |
| `POST /actuator/outboxadmin` body `{"eventType": "X", "limit": 100}` | bulk re-enable; `eventType` required. With `"action": "replay"` (+ optional ISO-instant `archivedAfter` / `archivedBefore` window and a `cursor`): bulk replay from the archive; response `{"replayed": n, "coalesced": n, "idInUse": n, "nextCursor": "…"}`. Sweep by feeding `nextCursor` back as `cursor` until it comes back null — rows that stay archived are counted but never block the walk |
| `DELETE /actuator/outboxadmin?target=disabled&olderThanDays=90&limit=1000` | purge; `target` = `disabled` \| `archive`, `olderThanDays` required |

Event payloads appear as `payload` (text formats, verbatim JSON) or
`payloadBase64` (binary formats) — exactly one is set
([ADR-0025](../adr/0025-binary-capable-serializer-spi-and-payload-format.md)).
The pagination `cursor` is opaque (`<iso-instant>_<uuid>`); pass back
`nextCursor` until it is `null`.

The endpoint activates only when `OutboxAdmin` and `EventStore` beans
exist — with the PostgreSQL adapter the starter wires
`PostgresOutboxAdmin` automatically. Note the in-memory adapter has no
archive: `findInArchive` is empty, archive purge is a no-op and replay
reports `NOT_FOUND` / zero counts.

## When to use it

- You run Spring Boot Actuator on a management port — this is the
  natural place for an ops surface: no new property namespace, JMX
  exposure and security fall out of your existing Actuator setup.
- Prefer [admin-rest](event-outboxer-admin-rest.md) when Actuator is
  not exposed and you need the surface on the application port with an
  authority-based guard.
- For *automatic* cleanup on a schedule, you don't need either module —
  configure `event-outboxer.retention.*`
  ([CONFIGURATION.md](../CONFIGURATION.md#event-outboxerretention)),
  which runs over the same `OutboxAdmin` port inside the engine.

## How to configure it

```xml
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-admin-actuator</artifactId>
</dependency>
```

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health, outboxadmin   # not exposed by default
```

There are no `event-outboxer.*` properties of its own.

**Security:** the endpoint is write-capable and destructive
(re-enable, replay, purge). Do not expose it on a public port; secure it like
any other sensitive Actuator endpoint — separate management port
and/or a `SecurityFilterChain` rule:

```java
http.securityMatcher(EndpointRequest.to("outboxadmin"))
    .authorizeHttpRequests(a -> a.anyRequest().hasRole("OUTBOX_ADMIN"));
```

## Related

- [event-outboxer-admin-rest](event-outboxer-admin-rest.md) — the app-port alternative.
- [STORAGE.md §Admin API](../STORAGE.md#admin-api-preferred-over-raw-sql) — why the API beats raw SQL.
- [ADR-0019](../adr/0019-admin-and-retention-surface.md).
