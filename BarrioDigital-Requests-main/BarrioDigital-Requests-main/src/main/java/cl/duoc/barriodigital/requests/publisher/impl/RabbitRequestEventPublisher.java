package cl.duoc.barriodigital.requests.publisher.impl;

import cl.duoc.barriodigital.requests.config.rabbit.RabbitMQProperties;
import cl.duoc.barriodigital.requests.event.RequestCreatedEvent;
import cl.duoc.barriodigital.requests.event.RequestStatusChangedEvent;
import cl.duoc.barriodigital.requests.publisher.RequestEventPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Implementación de RequestEventPublisher basada en RabbitTemplate.
 * Utiliza los nombres de exchange y routing keys centralizados en RabbitMQProperties.
 */
@Component
@ConditionalOnProperty(name = "spring.rabbitmq.host")
@RequiredArgsConstructor
@Slf4j
public class RabbitRequestEventPublisher implements RequestEventPublisher {

    private final RabbitTemplate rabbitTemplate;
    private final RabbitMQProperties properties;

    @Override
    public void publishRequestCreated(RequestCreatedEvent event) {
        String exchange = properties.getExchanges().getMain();

        // 1. Enviar evento a la cola de Notificaciones al Vecino
        String notifKey = properties.getRoutingKeys().getNotificationCreated();
        log.info("Publicando evento RequestCreated a Exchange '{}' con RoutingKey '{}' [Tracking: {}]",
                exchange, notifKey, event.getTrackingNumber());
        try {
            rabbitTemplate.convertAndSend(exchange, notifKey, event);
        } catch (Exception e) {
            log.error("Error al publicar evento de notificación en RabbitMQ: {}", e.getMessage());
        }

        // 2. Enviar evento a la cola de Generación de Tickets Comanales
        String ticketKey = properties.getRoutingKeys().getTicketGenerate();
        log.info("Publicando evento TicketGeneration a Exchange '{}' con RoutingKey '{}' [Tracking: {}]",
                exchange, ticketKey, event.getTrackingNumber());
        try {
            rabbitTemplate.convertAndSend(exchange, ticketKey, event);
        } catch (Exception e) {
            log.error("Error al publicar evento de generación de ticket en RabbitMQ: {}", e.getMessage());
        }
    }

    @Override
    public void publishStatusChanged(RequestStatusChangedEvent event) {
        String exchange = properties.getExchanges().getMain();
        String statusKey = properties.getRoutingKeys().getStatusChanged();

        log.info("Publicando evento RequestStatusChanged a Exchange '{}' con RoutingKey '{}' [Tracking: {}, Nuevo Estado: {}]",
                exchange, statusKey, event.getTrackingNumber(), event.getNewStatus());
        try {
            rabbitTemplate.convertAndSend(exchange, statusKey, event);
        } catch (Exception e) {
            log.error("Error al publicar evento de cambio de estado en RabbitMQ: {}", e.getMessage());
        }
    }
}
