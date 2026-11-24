package cl.duoc.barriodigital.admin.controller;

import cl.duoc.barriodigital.admin.dto.CreateQueueDto;
import cl.duoc.barriodigital.admin.dto.QueueInfoDto;
import cl.duoc.barriodigital.admin.service.RabbitAdminService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Controlador REST para la administración de Colas en RabbitMQ.
 * Expone endpoints siguiendo las mejores prácticas REST:
 * - POST /queues : Creación de colas validadas con DTO
 * - GET /queues : Listado de colas configuradas
 * - GET /queues/{name} : Consulta de estado de una cola
 * - DELETE /queues/{name} : Eliminación de colas
 * - POST /queues/{name}/purge : Purga de mensajes
 */
@RestController
@RequestMapping
@RequiredArgsConstructor
@Slf4j
@CrossOrigin(origins = "*")
public class RabbitQueueController {

    private final RabbitAdminService rabbitAdminService;

    @PostMapping({"/queues", "/api/queues"})
    public ResponseEntity<QueueInfoDto> createQueue(@Valid @RequestBody CreateQueueDto dto) {
        log.info("Solicitud REST para crear cola: {}", dto.getName());
        QueueInfoDto created = rabbitAdminService.createQueue(dto);
        return ResponseEntity.status(HttpStatus.CREATED).body(created);
    }

    @GetMapping({"/queues", "/api/queues"})
    public ResponseEntity<List<QueueInfoDto>> listQueues() {
        return ResponseEntity.ok(rabbitAdminService.listConfiguredQueues());
    }

    @GetMapping({"/queues/{name}", "/api/queues/{name}"})
    public ResponseEntity<QueueInfoDto> getQueueInfo(@PathVariable String name) {
        return ResponseEntity.ok(rabbitAdminService.getQueueInfo(name));
    }

    @DeleteMapping({"/queues/{name}", "/api/queues/{name}"})
    public ResponseEntity<?> deleteQueue(@PathVariable String name) {
        log.info("Solicitud REST para eliminar cola: {}", name);
        boolean deleted = rabbitAdminService.deleteQueue(name);
        if (deleted) {
            return ResponseEntity.ok(Map.of(
                    "status", "DELETED",
                    "queue", name,
                    "message", "Cola eliminada exitosamente del broker"
            ));
        } else {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                    "status", "NOT_FOUND",
                    "queue", name,
                    "message", "La cola no fue encontrada o ya estaba eliminada"
            ));
        }
    }

    @PostMapping({"/queues/{name}/purge", "/api/queues/{name}/purge"})
    public ResponseEntity<Map<String, Object>> purgeQueue(@PathVariable String name) {
        log.info("Solicitud REST para purgar cola: {}", name);
        rabbitAdminService.purgeQueue(name);
        return ResponseEntity.ok(Map.of(
                "status", "PURGED",
                "queue", name,
                "message", "Cola purgada correctamente"
        ));
    }
}
