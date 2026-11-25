package cl.duoc.barriodigital.requests.dto.kafka;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * DTO para streaming de eventos de ciclo de vida de solicitudes/trámites en Kafka.
 * Tópico: barriodigital.requests.events
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KafkaRequestEventDto implements Serializable {
    private String eventId;
    private String eventType; // e.g. REQUEST_CREATED, REQUEST_STATUS_CHANGED
    private Long requestId;
    private String trackingNumber;
    private String citizenDni;
    private String citizenName;
    private String procedureName;
    private String previousStatus;
    private String newStatus;
    private String performedBy;
    private LocalDateTime timestamp;
}
