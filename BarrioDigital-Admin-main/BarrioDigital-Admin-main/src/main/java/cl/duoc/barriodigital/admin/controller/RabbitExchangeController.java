package cl.duoc.barriodigital.admin.controller;

import cl.duoc.barriodigital.admin.dto.CreateExchangeDto;
import cl.duoc.barriodigital.admin.service.RabbitAdminService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * Controlador REST para la administración de Exchanges en RabbitMQ.
 * - POST /exchanges : Creación de exchanges validados
 * - DELETE /exchanges/{name} : Eliminación de exchange
 */
@RestController
@RequestMapping
@RequiredArgsConstructor
@Slf4j
@CrossOrigin(origins = "*")
public class RabbitExchangeController {

    private final RabbitAdminService rabbitAdminService;

    @PostMapping({"/exchanges", "/api/exchanges"})
    public ResponseEntity<Map<String, Object>> createExchange(@Valid @RequestBody CreateExchangeDto dto) {
        log.info("Solicitud REST para crear exchange: {}", dto.getName());
        rabbitAdminService.createExchange(dto);
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "status", "CREATED",
                "exchange", dto.getName(),
                "type", dto.getType().toUpperCase(),
                "durable", dto.isDurable(),
                "message", "Exchange creado exitosamente en el broker"
        ));
    }

    @DeleteMapping({"/exchanges/{name}", "/api/exchanges/{name}"})
    public ResponseEntity<?> deleteExchange(@PathVariable String name) {
        log.info("Solicitud REST para eliminar exchange: {}", name);
        boolean deleted = rabbitAdminService.deleteExchange(name);
        if (deleted) {
            return ResponseEntity.ok(Map.of(
                    "status", "DELETED",
                    "exchange", name,
                    "message", "Exchange eliminado exitosamente"
            ));
        } else {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                    "status", "NOT_FOUND",
                    "exchange", name,
                    "message", "El exchange no existe o no pudo ser eliminado"
            ));
        }
    }
}
