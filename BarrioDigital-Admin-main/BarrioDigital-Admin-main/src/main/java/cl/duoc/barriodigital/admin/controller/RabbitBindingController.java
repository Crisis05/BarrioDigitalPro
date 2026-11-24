package cl.duoc.barriodigital.admin.controller;

import cl.duoc.barriodigital.admin.dto.CreateBindingDto;
import cl.duoc.barriodigital.admin.service.RabbitAdminService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * Controlador REST para la administración de Enlaces (Bindings) en RabbitMQ.
 * - POST /bindings : Enlazar cola a exchange con routing key
 * - DELETE /bindings : Desenlazar cola de exchange
 */
@RestController
@RequestMapping
@RequiredArgsConstructor
@Slf4j
@CrossOrigin(origins = "*")
public class RabbitBindingController {

    private final RabbitAdminService rabbitAdminService;

    @PostMapping({"/bindings", "/api/bindings"})
    public ResponseEntity<Map<String, Object>> createBinding(@Valid @RequestBody CreateBindingDto dto) {
        log.info("Solicitud REST para crear binding entre cola '{}' y exchange '{}'",
                dto.getQueueName(), dto.getExchangeName());
        rabbitAdminService.createBinding(dto);
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "status", "CREATED",
                "queue", dto.getQueueName(),
                "exchange", dto.getExchangeName(),
                "routingKey", dto.getRoutingKey(),
                "message", "Binding creado exitosamente"
        ));
    }

    @DeleteMapping({"/bindings", "/api/bindings"})
    public ResponseEntity<Map<String, Object>> removeBinding(@Valid @RequestBody CreateBindingDto dto) {
        log.info("Solicitud REST para remover binding entre cola '{}' y exchange '{}'",
                dto.getQueueName(), dto.getExchangeName());
        rabbitAdminService.removeBinding(dto);
        return ResponseEntity.ok(Map.of(
                "status", "REMOVED",
                "queue", dto.getQueueName(),
                "exchange", dto.getExchangeName(),
                "routingKey", dto.getRoutingKey(),
                "message", "Binding removido exitosamente"
        ));
    }
}
