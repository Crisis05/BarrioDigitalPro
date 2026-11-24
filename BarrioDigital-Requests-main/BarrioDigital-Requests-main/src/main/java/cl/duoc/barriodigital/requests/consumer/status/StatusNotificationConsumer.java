package cl.duoc.barriodigital.requests.consumer.status;

import cl.duoc.barriodigital.requests.event.RequestStatusChangedEvent;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@ConditionalOnProperty(name = "spring.rabbitmq.host")
@Slf4j
public class StatusNotificationConsumer {

    @RabbitListener(queues = "${barriodigital.rabbitmq.queues.status}")
    public void handleStatusNotification(
            RequestStatusChangedEvent event,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag
    ) throws IOException {
        log.info("[ESTADOS] Notificando cambio de estado: {} [{} -> {}] (Tag: {})",
                event.getTrackingNumber(), event.getPreviousStatus(), event.getNewStatus(), deliveryTag);
        try {
            if (event.getCitizenEmail() == null || event.getCitizenEmail().isBlank()) {
                throw new IllegalArgumentException("No hay email para notificar cambio de estado del tramite " + event.getTrackingNumber());
            }
            log.info("Email enviado a '{}': tramite {} cambio a {}", event.getCitizenEmail(), event.getTrackingNumber(), event.getNewStatus());
            channel.basicAck(deliveryTag, false);
            log.info("[ACK] Notificacion de estado confirmada. Tag: {}", deliveryTag);
        } catch (IllegalArgumentException e) {
            log.error("[NACK->DLQ] Error en notificacion de estado {}: {}", event.getTrackingNumber(), e.getMessage());
            channel.basicNack(deliveryTag, false, false);
        } catch (Exception e) {
            log.error("[NACK->DLQ] Fallo inesperado en notificacion de estado: {}", e.getMessage());
            channel.basicNack(deliveryTag, false, false);
        }
    }
}