# Migration checklist — raising the baseline back to Java 25

## Why this document exists

ADR-0017 was amended on 2026-04-22 to lower the library baseline from
Java 25 to Java 17. The move widened the reachable audience but left
a small, well-defined set of Java-17-compatibility concessions in the
code: an `instanceof`-chain in place of pattern-matching `switch` on
sealed types, and a reflection helper that invokes
`Thread.ofVirtual()` / `Executors.newThreadPerTaskExecutor(...)`
instead of calling them directly.

This doc is the "how to undo those concessions in one PR" checklist
for the day the baseline moves back to Java 25 (or any ≥ 21 version
that makes them unnecessary). Written while the concessions are
fresh, so nothing is forgotten.

## When to revisit

Revisit this migration when **any** of:

1. Java 17 reaches end-of-public-support (Oct 2029 per Oracle's LTS
   schedule) and adoption data shows <10% of deployments still on 17.
2. A Java 21+ language feature becomes load-bearing for the design
   (e.g. scoped values for per-event-type `ThreadLocal` replacement,
   structured concurrency for coordinated shutdown, record patterns
   in failure decisioning).
3. The reflection gate accrues enough JVM-version-specific tweaks
   that maintenance cost starts to exceed the audience-reach benefit.

A move to Java **21** (rather than 25) baseline is intermediate —
it buys back native virtual-thread syntax and pattern-matching
`switch`, but JEP 491 (no `synchronized` pinning) still arrives only
on a JDK 25+ runtime. Same mechanical changes below apply; the only
difference is the `maven.compiler.release` target value.

## Scope

Only the lowering-for-audience concessions. **Not** in scope:
- Rewriting modules, APIs, or packaging.
- Adopting new Java 21+ features that weren't previously used.
- Bumping Spring Boot, PostgreSQL, Redis, or other external versions.

## Build configuration changes

### `pom.xml` (parent)

```xml
<!-- before -->
<java.version>17</java.version>
<maven.compiler.source>${java.version}</maven.compiler.source>
<maven.compiler.target>${java.version}</maven.compiler.target>
<maven.compiler.release>${java.version}</maven.compiler.release>

<!-- after -->
<java.version>25</java.version>
<maven.compiler.source>${java.version}</maven.compiler.source>
<maven.compiler.target>${java.version}</maven.compiler.target>
<maven.compiler.release>${java.version}</maven.compiler.release>
```

Also in `maven-enforcer-plugin` configuration:

```xml
<!-- before -->
<requireJavaVersion>
    <version>[17,)</version>
</requireJavaVersion>

<!-- after -->
<requireJavaVersion>
    <version>[25,)</version>
</requireJavaVersion>
```

Drop the long comment block that was introduced above
`<java.version>` explaining the Java-17-with-opt-ins stance.

### `examples/spring-boot-postgres/pom.xml`

```xml
<!-- before -->
<java.version>17</java.version>
<maven.compiler.release>17</maven.compiler.release>

<!-- after -->
<java.version>25</java.version>
<maven.compiler.release>25</maven.compiler.release>
```

## Java source changes

### `event-outboxer-core/…/dispatch/HandlerDispatcher.java`

Two `instanceof`-chain routing sites to convert back to
pattern-matching `switch`.

**`routeOutcome(...)` — `EventOutcome` dispatch:**

```java
// before — Java 17 compatibility form
if (outcome instanceof EventOutcome.Success) {
  finaliseSuccess(claimed);
} else if (outcome instanceof EventOutcome.Skip skip) {
  finaliseSkip(claimed, skip);
} else if (outcome instanceof EventOutcome.Retry retry) {
  handleRetryOutcome(claimed, handler, payload, retry);
} else if (outcome instanceof EventOutcome.Fail fail) {
  handleFailOutcome(claimed, handler, payload, fail);
} else {
  throw new IllegalStateException("unhandled EventOutcome: " + outcome.getClass());
}

// after — pattern-matching switch, compiler-enforced exhaustiveness
switch (outcome) {
  case EventOutcome.Success ignored -> finaliseSuccess(claimed);
  case EventOutcome.Skip skip -> finaliseSkip(claimed, skip);
  case EventOutcome.Retry retry -> handleRetryOutcome(claimed, handler, payload, retry);
  case EventOutcome.Fail fail -> handleFailOutcome(claimed, handler, payload, fail);
}
```

**`applyFailureDecision(...)` — `FailureDecision` dispatch:**

```java
// before
if (decision instanceof FailureDecision.RetryAt ra) {
  applyRetry(claimed, ra.when(), ra.reason(), cause);
} else if (decision instanceof FailureDecision.Disable d) {
  finaliseDisable(claimed, d.reason(), cause);
} else if (decision instanceof FailureDecision.Delete del) {
  applyDelete(claimed, del.reason());
} else {
  throw new IllegalStateException("unhandled FailureDecision: " + decision.getClass());
}

// after
switch (decision) {
  case FailureDecision.RetryAt ra -> applyRetry(claimed, ra.when(), ra.reason(), cause);
  case FailureDecision.Disable d -> finaliseDisable(claimed, d.reason(), cause);
  case FailureDecision.Delete del -> applyDelete(claimed, del.reason());
}
```

Also drop the two-line comment that begins "instanceof chain instead
of pattern-matching switch…" above `routeOutcome`.

### `event-outboxer-api/…/handle/EventOutcomeTest.java`

One test method to rename and convert. Sealed-type completeness is
again a compile-time guarantee — the runtime `throw` falls away.

```java
// before
@Test
void sealedInstanceofRoutingHitsCorrectBranch() {
  EventOutcome outcome = new EventOutcome.Retry("boom", Duration.ofSeconds(5), null);
  String label;
  if (outcome instanceof EventOutcome.Success) {
    label = "success";
  } else if (outcome instanceof EventOutcome.Retry r) {
    label = "retry:" + r.reason();
  } else if (outcome instanceof EventOutcome.Fail f) {
    label = "fail:" + f.reason();
  } else if (outcome instanceof EventOutcome.Skip s) {
    label = "skip:" + s.reason();
  } else {
    throw new IllegalStateException("unhandled EventOutcome: " + outcome.getClass());
  }
  assertThat(label).isEqualTo("retry:boom");
}

// after
@Test
void sealedPatternMatchIsExhaustive() {
  EventOutcome outcome = new EventOutcome.Retry("boom", Duration.ofSeconds(5), null);
  // If a new subtype is added to EventOutcome, this switch fails to compile.
  String label =
      switch (outcome) {
        case EventOutcome.Success s -> "success";
        case EventOutcome.Retry r -> "retry:" + r.reason();
        case EventOutcome.Fail f -> "fail:" + f.reason();
        case EventOutcome.Skip s -> "skip:" + s.reason();
      };
  assertThat(label).isEqualTo("retry:boom");
}
```

### `event-outboxer-spring-boot-starter/…/executor/HandlerExecutorFactory.java`

Delete the reflection helper and inline the native calls.

```java
// before — reflection gate
public static Function<EventTypeConfig, ExecutorService> virtual(TaskDecorator decorator) {
  Objects.requireNonNull(decorator, "decorator must not be null");
  return cfg ->
      new ContextPropagatingExecutorService(newVirtualThreadPerTaskExecutor(), decorator);
}

private static ExecutorService newVirtualThreadPerTaskExecutor() {
  try {
    Class<?> builderCls = Class.forName("java.lang.Thread$Builder$OfVirtual");
    Method ofVirtual = Thread.class.getMethod("ofVirtual");
    Method name = builderCls.getMethod("name", String.class, long.class);
    Method factory = builderCls.getMethod("factory");
    Method newThreadPerTask =
        java.util.concurrent.Executors.class.getMethod(
            "newThreadPerTaskExecutor", ThreadFactory.class);

    Object builder = ofVirtual.invoke(null);
    builder = name.invoke(builder, "outbox-vt-", 0L);
    ThreadFactory tf = (ThreadFactory) factory.invoke(builder);
    return (ExecutorService) newThreadPerTask.invoke(null, tf);
  } catch (NoSuchMethodException | ClassNotFoundException ex) {
    throw new IllegalStateException(
        "virtual-thread handler executor requires JDK 21+ at runtime; current JVM is "
            + Runtime.version()
            + ". Use 'event-outboxer.handler-executor.type=platform' (default) or upgrade"
            + " the runtime. For JEP 491 (no synchronized pinning), use JDK 25+.",
        ex);
  } catch (ReflectiveOperationException ex) {
    throw new IllegalStateException(
        "failed to invoke Thread.ofVirtual() via reflection — unexpected for JDK "
            + Runtime.version(),
        ex);
  }
}

// after — native calls, no reflection
public static Function<EventTypeConfig, ExecutorService> virtual(TaskDecorator decorator) {
  Objects.requireNonNull(decorator, "decorator must not be null");
  return cfg ->
      new ContextPropagatingExecutorService(
          Executors.newThreadPerTaskExecutor(
              Thread.ofVirtual().name("outbox-vt-", 0L).factory()),
          decorator);
}
```

**Imports:**
- Remove `import java.lang.reflect.Method;`.
- Remove `import java.util.concurrent.ThreadFactory;` (only used inside the reflection helper).
- Add back `import java.util.concurrent.Executors;` if removed.

**Class Javadoc:**
- Drop the note about "reflection invocation" and "fails fast with a
  clear error on JDK < 21". The `virtual()` method Javadoc can go back
  to the original one-liner pointing at JEP 491.

### `event-outboxer-spring-boot-starter/…/executor/ContextPropagatingExecutorServiceTest.java`

Optional — the test currently exercises the decorator over a
platform-thread executor, which is semantically equivalent. If you
want it to exercise the virtual-thread path natively again:

```java
// optional post-migration state — back to virtual threads
ExecutorService delegate =
    Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("test-vt-", 0L).factory());
```

Drop the "Uses a platform-thread executor so the test compiles on the
Java 17 baseline" comment.

## Doc changes

Single-line search-and-replace for strings that explicitly mention
the Java 17 baseline. Approximate list:

| File | What to update |
|---|---|
| `README.md` | Top-line stack: "Java 17+" → "Java 25"; drop the two-line JDK-21/25 qualifier |
| `CLAUDE.md` §Stack | "Java 17 baseline" block → "Java 25 (LTS)" one-liner, drop the reflection / JDK-21-opt-in paragraph |
| `CHANGELOG.md` | Initial-release header ("Java 17+") and the bullet points mentioning reflection-gated virtual threads |
| `docs/CONFIGURATION.md` | `event-outboxer.handler-executor.type=virtual` description — drop "Requires JDK 21+ at runtime" qualifier |
| `docs/adr/0009-spring-task-executor-in-starter.md` | Three spots: Override-points section, Rationale bullet on virtual threads, Implementation-notes virtual-threads paragraph — restore "Java 25 baseline" wording |
| `docs/adr/0016-maven-module-structure.md` | "Java 17 (baseline)" → "Java 25" |
| `docs/adr/0017-java-25-and-spring-boot-3-5-baseline.md` | Add a third amendment history entry with the revert date + rationale. Keep the 2026-04-22 intermediate amendment as historical record (don't delete it) |
| `docs/adr/README.md` | ADR-0017 title |
| `examples/spring-boot-postgres/README.md` | Prerequisites: JDK 17+ → JDK 25 |

Global gitignore note (`*.md` in `~/.gitignore_global` — user env): a
few of these docs may be untracked and require `git add -f`. Known
in-tree but locally-ignored: `CLAUDE.md`, `CHANGELOG.md`,
`docs/OBSERVABILITY.md`. Run `git ls-files` to confirm.

## ADR amendment (mandatory)

Add a new entry to ADR-0017 `§Amendment history`:

```markdown
- **YYYY-MM-DD (revert)** — baseline raised back to Java 25. All
  Java-17-compatibility concessions documented in
  `docs/MIGRATION-TO-JAVA-25.md` removed in the same PR. Rationale:
  <the trigger from §When to revisit above>.
```

Also update the ADR `## Status` / `## Date` block and its `## Context`
intro so it reads as a forward-looking "Java 25 baseline" document
again, with amendment history showing the detour.

## Verification

```bash
# Full reactor must stay green; switch target is what matters.
./mvnw -B -ntp clean verify

# IT suite — unchanged, but run it to confirm.
./mvnw -B -ntp clean verify -P it

# No leftover reflection helper or instanceof-chain concession markers.
grep -R "newVirtualThreadPerTaskExecutor\|requires JDK 21+ at runtime" .
# → expected: 0 hits in source; historical mentions in this doc are fine.

# No leftover "instanceof chain instead of pattern-matching switch" comment.
grep -R "instanceof chain instead of pattern-matching" .
# → expected: 0

# ContextPropagatingExecutorServiceTest compiles on the new release.
# (If you reverted it to virtual threads, the test suite also runs it natively.)

# Release target is what we set.
grep -R "maven.compiler.release" pom.xml examples/*/pom.xml
# → expected: all show 25
```

## What we regain

1. **Native syntax where it helps most.** Pattern-matching `switch`
   on sealed types in `HandlerDispatcher` is idiomatic Java 21+ and
   the compiler guarantees exhaustiveness — a new `EventOutcome`
   subtype would be caught at compile time, not at runtime.
2. **No reflection helper.** 30 lines of brittle code deleted;
   `HandlerExecutorFactory.virtual()` reads like regular Java 21+
   code.
3. **JEP 491 unconditional.** `event-outboxer.handler-executor.type=virtual`
   now works everywhere, no runtime guard needed. Users can safely
   put it in production without having to reason about JDK version
   mismatches.
4. **Future Java 21+ features are free to reach for.** Scoped values
   (JEP 492), structured concurrency (JEP 499), record patterns,
   virtual-thread-aware APIs — no new reflection islands required.

## What does NOT need changing

- `event-outboxer-cache-redis` module — independent of the Java
  baseline.
- `MetricsSnapshotCache` SPI and all its auto-configurations.
- All property paths (`event-outboxer.*`).
- All tests except the two mentioned above.
- Spring Boot 3.5.6 pinning (still fits; Boot 3.x requires Java 17+
  but is happy on Java 25).

## Estimated effort

- Pure mechanical: ~2 hours (build config, code edits, doc sweep).
- Build + full IT verification: ~30 minutes.
- ADR amendment + history polish: ~30 minutes.

Total: half a day of focused work. Most of the risk is catching every
doc mention of "Java 17 baseline"; the compiler (with the new
`release=25`) will catch any code path we missed.
