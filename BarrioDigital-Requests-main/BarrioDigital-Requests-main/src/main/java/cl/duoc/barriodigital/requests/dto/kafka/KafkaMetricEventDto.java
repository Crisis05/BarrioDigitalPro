package cl.duoc.barriodigital.requests.dto.kafka;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * DTO para eventos de analítica y métricas comunales en tiempo real en Kafka.
 * Tópico: barriodigital.analytics.metrics
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KafkaMetricEventDto implements Serializable {
    private String metricId;
    private String metricName; // e.g. TRAMITE_INGRESADO, CAMBIO_ESTADO
    private String procedureName;
    private String currentStatus;
    private Long processingTimeSeconds;
    private LocalDateTime recordedAt;
}
