package com.example.outbox;

import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

  private final OrderService orderService;
  private final OrderRepository orderRepository;

  public OrderController(OrderService orderService, OrderRepository orderRepository) {
    this.orderService = orderService;
    this.orderRepository = orderRepository;
  }

  public record CreateOrderRequest(String customerId, String email, long totalCents) {}

  public record CreateOrderResponse(UUID orderId) {}

  @PostMapping
  public CreateOrderResponse create(@RequestBody CreateOrderRequest request) {
    UUID id =
        orderService.createOrder(
            request.customerId(), request.email(), request.totalCents());
    return new CreateOrderResponse(id);
  }

  @GetMapping("/{id}")
  public ResponseEntity<Order> find(@PathVariable UUID id) {
    return orderRepository.findById(id).map(ResponseEntity::ok)
        .orElseGet(() -> ResponseEntity.notFound().build());
  }
}
