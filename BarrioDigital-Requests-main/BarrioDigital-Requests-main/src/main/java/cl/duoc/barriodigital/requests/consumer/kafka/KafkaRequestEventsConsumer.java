package cl.duoc.barriodigital.requests.consumer.kafka;

import cl.duoc.barriodigital.requests.dto.kafka.KafkaRequestEventDto;
import cl.duoc.barriodigital.requests.service.kafka.KafkaStreamingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumidor Kafka para el tópico de eventos de solicitudes.
 * Consumer Group: barriodigital-audit-group
 * Tópico: barriodigital.requests.events
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaRequestEventsConsumer {

    private final KafkaStreamingService streamingService;

    @KafkaListener(
            topics = "${barriodigital.kafka.topics.requests-events:barriodigital.requests.events}",
            groupId = "${barriodigital.kafka.consumer-groups.audit:barriodigital-audit-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeRequestEvent(
            @Payload KafkaRequestEventDto payload,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("[KAFKA CONSUMER AUDIT] Evento recibido desde partición {} con offset {}: {}",
                partition, offset, payload);

        // Validación defensiva del payload
        if (payload == null || payload.getTrackingNumber() == null) {
            log.warn("[KAFKA CONSUMER AUDIT] Payload inválido o vacío descartado. Offset: {}", offset);
            return;
        }

        try {
            // Delegar la lógica de negocio al servicio de dominio
            streamingService.processRequestEvent(payload);
        } catch (Exception e) {
            log.error("[KAFKA CONSUMER AUDIT] Error procesando evento de trámite {}: {}",
                    payload.getTrackingNumber(), e.getMessage(), e);
            throw e; // Permite que el errorHandler aplique reintentos controlados
        }
    }
}
