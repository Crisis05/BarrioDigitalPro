package cl.duoc.barriodigital.requests.consumer.notification;

import cl.duoc.barriodigital.requests.event.RequestCreatedEvent;
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
public class NotificationConsumer {

    @RabbitListener(queues = "${barriodigital.rabbitmq.queues.notifications}")
    public void handleNotificationMessage(
            RequestCreatedEvent event,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag
    ) throws IOException {
        log.info("[NOTIFICACIONES] Recibido tramite: {} (Tag: {})", event.getTrackingNumber(), deliveryTag);
        try {
            if (event.isSimulateError()) {
                throw new IllegalArgumentException("Error simulado para demostracion de DLQ");
            }
            if (event.getCitizenEmail() == null || !event.getCitizenEmail().contains("@")) {
                throw new IllegalArgumentException("Email invalido: " + event.getCitizenEmail());
            }
            log.info("Notificacion enviada a '{}' para tramite: {}", event.getCitizenEmail(), event.getTrackingNumber());
            channel.basicAck(deliveryTag, false);
            log.info("[ACK] Notificacion confirmada. Tag: {}", deliveryTag);
        } catch (IllegalArgumentException e) {
            log.error("[NACK->DLQ] Falla en notificacion {}: {}", event.getTrackingNumber(), e.getMessage());
            channel.basicNack(deliveryTag, false, false);
        } catch (Exception e) {
            log.error("[NACK->DLQ] Error general en notificacion: {}", e.getMessage(), e);
            channel.basicNack(deliveryTag, false, false);
        }
    }
}