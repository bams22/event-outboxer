package com.example.outbox;

import io.github.bams22.outboxer.api.publish.OutboxEventPublisher;
import java.time.Instant;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Demonstrates the core outbox guarantee: the INSERT into {@code orders} and the publish of
 * {@code SEND_ORDER_CONFIRMATION} commit atomically. If anything later in the method throws, the
 * transaction rolls back and the event is never visible to the engine.
 */
@Service
public class OrderService {

  private final OrderRepository orderRepository;
  private final OutboxEventPublisher publisher;

  public OrderService(OrderRepository orderRepository, OutboxEventPublisher publisher) {
    this.orderRepository = orderRepository;
    this.publisher = publisher;
  }

  @Transactional
  public UUID createOrder(String customerId, String email, long totalCents) {
    UUID orderId = UUID.randomUUID();
    Order order = new Order(orderId, customerId, email, totalCents, Instant.now());
    orderRepository.save(order);

    publisher.publish(
        SendOrderConfirmationPayload.EVENT_TYPE,
        new SendOrderConfirmationPayload(orderId, email, totalCents));

    return orderId;
  }
}
