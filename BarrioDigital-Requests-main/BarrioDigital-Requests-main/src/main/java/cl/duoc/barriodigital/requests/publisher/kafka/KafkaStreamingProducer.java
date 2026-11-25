package cl.duoc.barriodigital.requests.publisher.kafka;

import cl.duoc.barriodigital.requests.config.kafka.KafkaProperties;
import cl.duoc.barriodigital.requests.dto.kafka.KafkaMetricEventDto;
import cl.duoc.barriodigital.requests.dto.kafka.KafkaRequestEventDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Productor de streaming de eventos hacia Apache Kafka.
 * Publica en los dos tópicos configurados:
 * 1. barriodigital.requests.events (eventos de trámites con key = trackingNumber)
 * 2. barriodigital.analytics.metrics (métricas con key = procedureName)
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaStreamingProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaProperties kafkaProperties;

    /**
     * Publica evento de ciclo de vida del trámite en el tópico de eventos.
     */
    public void sendRequestEvent(KafkaRequestEventDto event) {
        String topic = kafkaProperties.getTopics().getRequestsEvents();
        String key = event.getTrackingNumber();

        log.info("[KAFKA PRODUCER] Publicando evento en tópico '{}' [Key: {}, EventType: {}]",
                topic, key, event.getEventType());

        try {
            kafkaTemplate.send(topic, key, event).whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("[KAFKA PRODUCER] Mensaje enviado exitosamente a '{}' en partición {} con offset {}",
                            topic,
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                } else {
                    log.error("[KAFKA PRODUCER] Error enviando mensaje a Kafka tópico '{}': {}",
                            topic, ex.getMessage());
                }
            });
        } catch (Exception e) {
            log.error("[KAFKA PRODUCER] Excepción al intentar enviar a Kafka: {}", e.getMessage());
        }
    }

    /**
     * Publica métricas de analítica en el tópico de métricas.
     */
    public void sendMetricEvent(KafkaMetricEventDto metric) {
        String topic = kafkaProperties.getTopics().getAnalyticsMetrics();
        String key = metric.getProcedureName() != null ? metric.getProcedureName() : "GENERAL";

        log.info("[KAFKA PRODUCER] Publicando métrica en tópico '{}' [Key: {}, Métrica: {}]",
                topic, key, metric.getMetricName());

        try {
            kafkaTemplate.send(topic, key, metric).whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("[KAFKA PRODUCER] Métrica enviada exitosamente a '{}' en partición {} con offset {}",
                            topic,
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                } else {
                    log.error("[KAFKA PRODUCER] Error enviando métrica a Kafka tópico '{}': {}",
                            topic, ex.getMessage());
                }
            });
        } catch (Exception e) {
            log.error("[KAFKA PRODUCER] Excepción al intentar enviar métrica a Kafka: {}", e.getMessage());
        }
    }
}
