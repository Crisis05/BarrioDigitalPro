package cl.duoc.barriodigital.admin.controller;

import cl.duoc.barriodigital.admin.dto.CreateKafkaTopicDto;
import cl.duoc.barriodigital.admin.dto.KafkaClusterInfoDto;
import cl.duoc.barriodigital.admin.dto.KafkaConsumerGroupDto;
import cl.duoc.barriodigital.admin.dto.KafkaTopicInfoDto;
import cl.duoc.barriodigital.admin.service.KafkaAdminService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Controlador REST para la administración de Apache Kafka.
 * Expone operaciones requeridas por la pauta de evaluación:
 * - Crear tópicos con configuraciones.
 * - Listar y eliminar tópicos.
 * - Inspeccionar consumer groups.
 * - Consultar estado del clúster de brokers.
 */
@RestController
@RequestMapping("/api/admin/kafka")
@RequiredArgsConstructor
@Slf4j
@CrossOrigin(origins = "*")
public class KafkaAdminController {

    private final KafkaAdminService kafkaAdminService;

    @GetMapping("/topics")
    public ResponseEntity<List<KafkaTopicInfoDto>> listTopics() {
        return ResponseEntity.ok(kafkaAdminService.listTopics());
    }

    @PostMapping("/topics")
    public ResponseEntity<Map<String, Object>> createTopic(@Valid @RequestBody CreateKafkaTopicDto dto) {
        kafkaAdminService.createTopic(dto);
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "status", "CREATED",
                "topic", dto.getName(),
                "partitions", dto.getPartitions(),
                "replicationFactor", dto.getReplicationFactor(),
                "message", "Tópico Kafka creado exitosamente"
        ));
    }

    @DeleteMapping("/topics/{topicName}")
    public ResponseEntity<Map<String, Object>> deleteTopic(@PathVariable String topicName) {
        kafkaAdminService.deleteTopic(topicName);
        return ResponseEntity.ok(Map.of(
                "status", "DELETED",
                "topic", topicName,
                "message", "Tópico Kafka eliminado exitosamente"
        ));
    }

    @GetMapping("/consumer-groups")
    public ResponseEntity<List<KafkaConsumerGroupDto>> listConsumerGroups() {
        return ResponseEntity.ok(kafkaAdminService.listConsumerGroups());
    }

    @GetMapping("/cluster-info")
    public ResponseEntity<KafkaClusterInfoDto> getClusterInfo() {
        return ResponseEntity.ok(kafkaAdminService.getClusterInfo());
    }
}
