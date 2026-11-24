package cl.duoc.barriodigital.admin.controller;

import cl.duoc.barriodigital.admin.dto.PublishMessageDto;
import cl.duoc.barriodigital.admin.service.RabbitAdminService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * Controlador REST para emitir mensajes de prueba y verificar el comportamiento del broker y DLQ.
 */
@RestController
@RequestMapping
@RequiredArgsConstructor
@Slf4j
@CrossOrigin(origins = "*")
public class RabbitMessageController {

    private final RabbitAdminService rabbitAdminService;

    @PostMapping({"/messages/publish", "/api/messages/publish"})
    public ResponseEntity<Map<String, Object>> publishMessage(@Valid @RequestBody PublishMessageDto dto) {
        log.info("Solicitud REST para publicar mensaje de prueba a exchange '{}' con routingKey '{}'",
                dto.getExchange(), dto.getRoutingKey());
        rabbitAdminService.publishMessage(dto);
        return ResponseEntity.ok(Map.of(
                "status", "PUBLISHED",
                "exchange", dto.getExchange() != null ? dto.getExchange() : "(default)",
                "routingKey", dto.getRoutingKey(),
                "message", "Mensaje publicado al broker RabbitMQ exitosamente"
        ));
    }
}
