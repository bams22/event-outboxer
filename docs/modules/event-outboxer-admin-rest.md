# event-outboxer-admin-rest

An **opt-in** REST controller over the `OutboxAdmin` SPI
([ADR-0019](../adr/0019-admin-and-retention-surface.md)) — the same
operations as
[`event-outboxer-admin-actuator`](event-outboxer-admin-actuator.md),
but on the **application port**, guarded by a configurable
`@PreAuthorize` authority. For setups where Actuator is not exposed.

| | |
|---|---|
| Coordinates | `io.github.bams22:event-outboxer-admin-rest` |
| Java package | `io.github.bams22.outboxer.admin.rest` |
| Depends on | [`event-outboxer-api`](event-outboxer-api.md), [`event-outboxer-spi`](event-outboxer-spi.md), `spring-webmvc` (`spring-security-core` optional) |
| Enable with | `event-outboxer.admin.rest.enabled: true` (default **false**) |

## What it does

`OutboxAdminController` under the configurable base path
(default `/outbox-admin`):

| Verb + path | Purpose |
|---|---|
| `GET /events?status=DISABLED&type=X&limit=50&cursor=…` | list events (keyset pagination, newest first) |
| `GET /events/{id}` | one event — active store, then archive; 404 when absent |
| `POST /events/{id}/reenable` | re-enable one `DISABLED` event (fresh attempts budget); 409 when the row exists but is not `DISABLED` |
| `POST /events/reenable-all` body `{"eventType": "X", "createdBefore": …, "limit": 100}` | bulk re-enable |
| `POST /events/{id}/replay` | replay one archived event back into the hot table ([ADR-0033](../adr/0033-archive-dedup-key-and-replay-from-archive.md)); 200 `{"outcome": "REPLAYED"/"COALESCED"}` (a coalesced replay keeps the archive row), 404 when not in the archive, 409 when the hot table already holds that id (the app re-published the UUID — the live event is the one to look at) |
| `POST /events/replay-all` body `{"eventType": "X", "archivedAfter": …, "archivedBefore": …, "limit": 100, "cursor": …}` | bulk replay from the archive; response `{"replayed": n, "coalesced": n, "idInUse": n, "nextCursor": "…"}`. Sweep by feeding `nextCursor` back as `cursor` until it comes back null; the counters report rows that stayed archived and never block the walk |
| `POST /purge/disabled` body `{"olderThan": "<instant>", "eventType": …, "limit": 1000}` | purge old `DISABLED` events |
| `POST /purge/archive` body `{"archivedBefore": "<instant>", "limit": 1000}` | purge archive rows |

Responses are thin records (`EventResponse`, `EventPageResponse`,
`CountResponse`, `ReplayResponse`, `ReplayAllResponse`, …) so the
domain types can evolve without breaking the HTTP contract. Payloads
appear as `payload` (text) or `payloadBase64` (binary), exactly one
set; events carry their `dedupKey` when they have one.

A rejected argument — a malformed `cursor`, a non-positive `limit`, a
replay window whose `archivedAfter` is not strictly before
`archivedBefore` — comes back as **400** with the reason in
`{"error": …}`, not as a 500. The handler is scoped to this controller
rather than declared as a `@ControllerAdvice`, so enabling the admin
surface never changes how the host application renders its own
exceptions.

### Security model — the module's headline feature

- Every operation requires the authority named by
  `required-authority` (default `OUTBOX_ADMIN`) on the authenticated
  principal, enforced via `@PreAuthorize` + Spring **method
  security**. The permit name is configuration, not code.
- `@PreAuthorize` silently does nothing unless method security is
  active — so the module **fails startup** when Spring Security is on
  the classpath but `@EnableMethodSecurity` is missing, with a message
  telling you to add the annotation or explicitly accept an
  unprotected API via `enforce-authority: false`.
- No Spring Security on the classpath at all → the API runs
  unprotected (the accepted ADR-0019 trade-off for security-less
  apps — make that choice consciously).

## When to use it

- Actuator is not exposed (or policy forbids write operations on it)
  and you need the ops surface on the application port, integrated
  with your app's Spring Security setup.
- Otherwise prefer
  [admin-actuator](event-outboxer-admin-actuator.md) — the management
  port is the safer default home for a write-capable surface.
- Scheduled cleanup does not require either module — see
  `event-outboxer.retention.*`.

## How to configure it

```xml
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-admin-rest</artifactId>
</dependency>
```

```yaml
event-outboxer:
  admin:
    rest:
      enabled: true                    # default false — a write-capable HTTP surface must be deliberate
      base-path: /outbox-admin         # default
      required-authority: OUTBOX_ADMIN # default
      enforce-authority: true          # default; false = explicitly accept an unguarded API
```

And in your security configuration:

```java
@Configuration
@EnableMethodSecurity                  // required — startup fails without it (see above)
public class SecurityConfig {
    // grant OUTBOX_ADMIN to your ops principals
}
```

The controller registers only in servlet web apps and only when
`OutboxAdmin` + `EventStore` beans exist (wired automatically by the
[PostgreSQL storage adapter](event-outboxer-storage-postgres.md)).

Example calls:

```bash
curl -H "Authorization: Bearer $TOKEN" \
  'https://svc/outbox-admin/events?status=DISABLED&limit=20'

curl -X POST -H "Authorization: Bearer $TOKEN" \
  https://svc/outbox-admin/events/6f9d…/reenable

curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"eventType": "ORDER_SYNC", "archivedAfter": "2026-09-03T10:00:00Z", "archivedBefore": "2026-09-03T12:00:00Z"}' \
  https://svc/outbox-admin/events/replay-all
```

## Related

- [event-outboxer-admin-actuator](event-outboxer-admin-actuator.md) — the management-port sibling.
- [CONFIGURATION.md §admin](../CONFIGURATION.md#event-outboxeradminrest-and-the-admin-modules).
- [ADR-0019](../adr/0019-admin-and-retention-surface.md).
