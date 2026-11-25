package cl.duoc.barriodigital.requests.consumer.kafka;

import cl.duoc.barriodigital.requests.dto.kafka.KafkaMetricEventDto;
import cl.duoc.barriodigital.requests.service.kafka.KafkaStreamingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumidor Kafka para el tópico de métricas y analítica en streaming.
 * Consumer Group: barriodigital-analytics-group
 * Tópico: barriodigital.analytics.metrics
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaAnalyticsMetricsConsumer {

    private final KafkaStreamingService streamingService;

    @KafkaListener(
            topics = "${barriodigital.kafka.topics.analytics-metrics:barriodigital.analytics.metrics}",
            groupId = "${barriodigital.kafka.consumer-groups.analytics:barriodigital-analytics-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeMetricEvent(
            @Payload KafkaMetricEventDto metric,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("[KAFKA CONSUMER ANALYTICS] Métrica recibida desde partición {} con offset {}: {}",
                partition, offset, metric);

        // Validación defensiva del payload
        if (metric == null || metric.getMetricName() == null) {
            log.warn("[KAFKA CONSUMER ANALYTICS] Métrica inválida o sin nombre descartada. Offset: {}", offset);
            return;
        }

        try {
            // Delegar procesamiento al servicio de dominio
            streamingService.processMetricEvent(metric);
        } catch (Exception e) {
            log.error("[KAFKA CONSUMER ANALYTICS] Error registrando métrica {}: {}",
                    metric.getMetricName(), e.getMessage(), e);
            throw e; // ErrorHandler gestiona reintentos
        }
    }
}
