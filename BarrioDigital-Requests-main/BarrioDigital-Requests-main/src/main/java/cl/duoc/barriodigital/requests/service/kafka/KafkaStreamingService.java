package cl.duoc.barriodigital.requests.service.kafka;

import cl.duoc.barriodigital.requests.dto.kafka.KafkaMetricEventDto;
import cl.duoc.barriodigital.requests.dto.kafka.KafkaRequestEventDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Servicio de dominio para el procesamiento desacoplado de eventos y métricas de Kafka.
 * Separa la lógica de negocio de la infraestructura de mensajería (Evaluación 5).
 */
@Service
@Slf4j
public class KafkaStreamingService {

    // Contadores en memoria para agregaciones de streaming
    private final Map<String, AtomicLong> procedureMetrics = new ConcurrentHashMap<>();
    private final AtomicLong totalEventsProcessed = new AtomicLong(0);

    /**
     * Procesa el evento de trámite para auditoría y trazabilidad en tiempo real.
     */
    public void processRequestEvent(KafkaRequestEventDto event) {
        log.info("[DOMINIO STREAMING] Procesando evento de trámite: ID={}, Tipo={}, Trámite={}, Estado={}",
                event.getTrackingNumber(), event.getEventType(), event.getProcedureName(), event.getNewStatus());

        totalEventsProcessed.incrementAndGet();
    }

    /**
     * Procesa la métrica comunal para agregación de analítica en tiempo real.
     */
    public void processMetricEvent(KafkaMetricEventDto metric) {
        log.info("[DOMINIO ANALYTICS] Registrando métrica en streaming: Nombre={}, Procedimiento={}, Estado={}",
                metric.getMetricName(), metric.getProcedureName(), metric.getCurrentStatus());

        String key = metric.getProcedureName() != null ? metric.getProcedureName() : "General";
        procedureMetrics.computeIfAbsent(key, k -> new AtomicLong(0)).incrementAndGet();
    }

    public long getTotalEventsProcessed() {
        return totalEventsProcessed.get();
    }

    public Map<String, Long> getMetricsSummary() {
        Map<String, Long> summary = new ConcurrentHashMap<>();
        procedureMetrics.forEach((k, v) -> summary.put(k, v.get()));
        return summary;
    }
}
