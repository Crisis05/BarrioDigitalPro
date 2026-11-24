package cl.duoc.barriodigital.requests.consumer.ticket;

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
public class TicketGenerationConsumer {

    @RabbitListener(queues = "${barriodigital.rabbitmq.queues.tickets}")
    public void handleTicketGeneration(
            RequestCreatedEvent event,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag
    ) throws IOException {
        log.info("[TICKETS] Generando comprobante para tramite: {} (Tag: {})", event.getTrackingNumber(), deliveryTag);
        try {
            if (event.getTrackingNumber() == null || event.getTrackingNumber().isBlank()) {
                throw new IllegalArgumentException("Codigo de seguimiento invalido para emision de ticket");
            }
            String ticketCode = "TICKET-DIGITAL-" + event.getTrackingNumber();
            log.info("Comprobante generado con exito. Folio: {} para vecino: {}", ticketCode, event.getCitizenName());
            channel.basicAck(deliveryTag, false);
            log.info("[ACK] Ticket confirmado. Tag: {}", deliveryTag);
        } catch (IllegalArgumentException e) {
            log.error("[NACK->DLQ] Error al generar ticket {}: {}", event.getTrackingNumber(), e.getMessage());
            channel.basicNack(deliveryTag, false, false);
        } catch (Exception e) {
            log.error("[NACK->DLQ] Falla inesperada en generacion de ticket: {}", e.getMessage());
            channel.basicNack(deliveryTag, false, false);
        }
    }
}