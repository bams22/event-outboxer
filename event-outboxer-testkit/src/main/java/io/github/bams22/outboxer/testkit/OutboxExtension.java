/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.testkit;

import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

/**
 * JUnit 5 extension that provides a fresh {@link OutboxTestContext} per test method. Register via
 * {@code @ExtendWith(OutboxExtension.class)} and declare the context as a parameter:
 *
 * <pre>{@code
 * @ExtendWith(OutboxExtension.class)
 * class OrderHandlerTest {
 *
 *   @Test
 *   void processesOrder(OutboxTestContext outbox) {
 *     outbox.publisher().publish("ORDER", new OrderCreated(...));
 *     outbox.manualEngine().tick();
 *     assertThatStore(outbox.eventStore()).hasTotalPending(0);
 *   }
 * }
 * }</pre>
 *
 * <p>The default context has no handlers registered. Tests that need custom handlers can build
 * their own {@link OutboxTestContext} and skip the extension.
 */
public final class OutboxExtension implements ParameterResolver, BeforeEachCallback {

  private static final ExtensionContext.Namespace NS =
      ExtensionContext.Namespace.create(OutboxExtension.class);

  @Override
  public void beforeEach(ExtensionContext context) {
    // Context is created lazily on first parameter resolution; this hook just clears any
    // lingering state from prior runs.
    context.getStore(NS).remove("context");
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext) {
    return parameterContext.getParameter().getType() == OutboxTestContext.class;
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    ExtensionContext.Store store = extensionContext.getStore(NS);
    OutboxTestContext ctx = store.get("context", OutboxTestContext.class);
    if (ctx == null) {
      ctx = OutboxTestContext.builder().build();
      store.put("context", ctx);
    }
    return ctx;
  }
}
