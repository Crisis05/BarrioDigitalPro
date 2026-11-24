package cl.duoc.barriodigital.requests.controller;

import cl.duoc.barriodigital.requests.consumer.dlq.DeadLetterQueueConsumer;
import cl.duoc.barriodigital.requests.dto.CreateRequestDto;
import cl.duoc.barriodigital.requests.dto.UpdateStatusDto;
import cl.duoc.barriodigital.requests.event.DlqAuditRecord;
import cl.duoc.barriodigital.requests.event.RequestCreatedEvent;
import cl.duoc.barriodigital.requests.model.Request;
import cl.duoc.barriodigital.requests.model.RequestAuditLog;
import cl.duoc.barriodigital.requests.model.RequestStatus;
import cl.duoc.barriodigital.requests.publisher.RequestEventPublisher;
import cl.duoc.barriodigital.requests.service.RequestService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

@RestController
@RequestMapping("/api/requests")
@CrossOrigin(origins = "*")
public class RequestController {

    private final RequestService requestService;

    /** Puede ser null en tests (bean condicional a spring.rabbitmq.host) */
    @Autowired(required = false)
    private DeadLetterQueueConsumer dlqConsumer;

    /** Puede ser @MockBean en tests */
    @Autowired(required = false)
    private RequestEventPublisher eventPublisher;

    @Autowired
    public RequestController(RequestService requestService) {
        this.requestService = requestService;
    }

    @GetMapping
    public ResponseEntity<List<Request>> getRequests(
            @RequestParam(required = false) RequestStatus status,
            @RequestParam(required = false) String citizenDni,
            @RequestParam(required = false) String citizenEmail
    ) {
        return ResponseEntity.ok(requestService.getAllRequests(status, citizenDni, citizenEmail));
    }

    @GetMapping("/{id}")
    public ResponseEntity<Request> getRequestById(@PathVariable Long id) {
        return requestService.getRequestById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/tracking/{trackingNumber}")
    public ResponseEntity<Request> getByTrackingNumber(@PathVariable String trackingNumber) {
        return requestService.getRequestByTrackingNumber(trackingNumber)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping
    public ResponseEntity<?> createRequest(@Valid @RequestBody CreateRequestDto dto) {
        try {
            Request created = requestService.createRequest(dto);
            return ResponseEntity.status(HttpStatus.CREATED).body(created);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of(
                    "error", "ERROR_CREATE_REQUEST",
                    "message", e.getMessage() != null ? e.getMessage() : "Error al registrar la solicitud"
            ));
        }
    }

    @PutMapping("/{id}/status")
    public ResponseEntity<?> updateStatus(@PathVariable Long id, @Valid @RequestBody UpdateStatusDto dto) {
        try {
            Request updated = requestService.updateRequestStatus(id, dto);
            return ResponseEntity.ok(updated);
        } catch (NoSuchElementException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                    "error", "NOT_FOUND",
                    "message", e.getMessage()
            ));
        } catch (IllegalStateException e) {
            // Regla de negocio infringida: 422 Unprocessable Entity
            return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(Map.of(
                    "error", "BUSINESS_RULE_VIOLATION",
                    "message", e.getMessage()
            ));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "error", "INTERNAL_ERROR",
                    "message", e.getMessage()
            ));
        }
    }

    @GetMapping("/{id}/timeline")
    public ResponseEntity<List<RequestAuditLog>> getRequestTimeline(@PathVariable Long id) {
        return ResponseEntity.ok(requestService.getTimeline(id));
    }

    @GetMapping("/audit/all")
    public ResponseEntity<List<RequestAuditLog>> getAllAuditLogs() {
        return ResponseEntity.ok(requestService.getTimeline(null));
    }

    @GetMapping("/kpis")
    public ResponseEntity<Map<String, Object>> getKpis() {
        return ResponseEntity.ok(requestService.getKpis());
    }

    @GetMapping("/dlq/events")
    public ResponseEntity<List<DlqAuditRecord>> getDlqEvents() {
        if (dlqConsumer == null) {
            return ResponseEntity.ok(Collections.emptyList());
        }
        return ResponseEntity.ok(dlqConsumer.getRecordedDlqEvents());
    }

    @PostMapping("/test/simulate-error")
    public ResponseEntity<Map<String, Object>> simulateDlqError(
            @RequestParam(defaultValue = "BD-TEST-DLQ-9999") String trackingNumber
    ) {
        if (eventPublisher == null) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of(
                    "status", "NOT_AVAILABLE",
                    "message", "RabbitMQ no está configurado en este entorno"
            ));
        }

        RequestCreatedEvent failedEvent = RequestCreatedEvent.builder()
                .requestId(9999L)
                .trackingNumber(trackingNumber)
                .citizenDni("99.999.999-9")
                .citizenName("Vecino Prueba Fallo DLQ")
                .citizenEmail("invalido-sin-arroba")
                .procedureTypeId(1L)
                .procedureName("Prueba Controlada de Falla y NACK")
                .description("Mensaje forzado para demostrar basicNack y enrutamiento a DLQ")
                .createdBy("Auditor / Docente")
                .timestamp(LocalDateTime.now())
                .simulateError(true)
                .build();

        eventPublisher.publishRequestCreated(failedEvent);

        return ResponseEntity.ok(Map.of(
                "status", "DISPATCHED",
                "trackingNumber", trackingNumber,
                "message", "Evento con error provocado enviado a RabbitMQ. El consumidor ejecutará basicNack(requeue=false) y será derivado a DLQ."
        ));
    }
}
