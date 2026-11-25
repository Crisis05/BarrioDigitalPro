package cl.duoc.barriodigital.bff.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestClient;

import java.util.Map;

/**
 * BFF Controller que expone las operaciones de administración de RabbitMQ
 * hacia el frontend Angular, delegando en el microservicio ms-barriodigital-admin.
 */
@RestController
@RequestMapping("/api/admin")
@RequiredArgsConstructor
@Slf4j
@CrossOrigin(origins = "*")
public class BffRabbitAdminController {

    @Value("${services.admin.url:http://localhost:8083}")
    private String adminServiceUrl;

    @Value("${services.requests.url:http://localhost:8081}")
    private String requestsServiceUrl;

    private final RestClient restClient = RestClient.create();

    // ==========================================
    // COLAS (QUEUES)
    // ==========================================

    @GetMapping("/queues")
    public ResponseEntity<?> getQueues() {
        try {
            String json = restClient.get()
                    .uri(adminServiceUrl + "/api/queues")
                    .retrieve()
                    .body(String.class);
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(json);
        } catch (HttpClientErrorException | HttpServerErrorException e) {
            return ResponseEntity.status(e.getStatusCode()).body(e.getResponseBodyAsString());
        } catch (Exception e) {
            log.error("Error comunicando con ms-barriodigital-admin: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "SERVICE_UNAVAILABLE", "message", "Microservicio administrador no disponible: " + e.getMessage()));
        }
    }

    @PostMapping("/queues")
    public ResponseEntity<?> createQueue(@RequestBody Map<String, Object> body) {
        try {
            String json = restClient.post()
                    .uri(adminServiceUrl + "/api/queues")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .body(String.class);
            return ResponseEntity.status(HttpStatus.CREATED).contentType(MediaType.APPLICATION_JSON).body(json);
        } catch (HttpClientErrorException | HttpServerErrorException e) {
            return ResponseEntity.status(e.getStatusCode()).contentType(MediaType.APPLICATION_JSON).body(e.getResponseBodyAsString());
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of("error", "ERROR_CREATE_QUEUE", "message", e.getMessage()));
        }
    }

    @DeleteMapping("/queues/{name}")
    public ResponseEntity<?> deleteQueue(@PathVariable String name) {
        try {
            String json = restClient.delete()
                    .uri(adminServiceUrl + "/api/queues/" + name)
                    .retrieve()
                    .body(String.class);
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(json != null ? json : "{}");
        } catch (HttpClientErrorException | HttpServerErrorException e) {
            return ResponseEntity.status(e.getStatusCode()).body(e.getResponseBodyAsString());
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of("error", "ERROR_DELETE_QUEUE", "message", e.getMessage()));
        }
    }

    @PostMapping("/queues/{name}/purge")
    public ResponseEntity<?> purgeQueue(@PathVariable String name) {
        try {
            String json = restClient.post()
                    .uri(adminServiceUrl + "/api/queues/" + name + "/purge")
                    .retrieve()
                    .body(String.class);
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(json);
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of("error", "ERROR_PURGE_QUEUE", "message", e.getMessage()));
        }
    }

    // ==========================================
    // EXCHANGES
    // ==========================================

    @PostMapping("/exchanges")
    public ResponseEntity<?> createExchange(@RequestBody Map<String, Object> body) {
        try {
            String json = restClient.post()
                    .uri(adminServiceUrl + "/api/exchanges")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .body(String.class);
            return ResponseEntity.status(HttpStatus.CREATED).contentType(MediaType.APPLICATION_JSON).body(json);
        } catch (HttpClientErrorException | HttpServerErrorException e) {
            return ResponseEntity.status(e.getStatusCode()).body(e.getResponseBodyAsString());
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of("error", "ERROR_CREATE_EXCHANGE", "message", e.getMessage()));
        }
    }

    @DeleteMapping("/exchanges/{name}")
    public ResponseEntity<?> deleteExchange(@PathVariable String name) {
        try {
            String json = restClient.delete()
                    .uri(adminServiceUrl + "/api/exchanges/" + name)
                    .retrieve()
                    .body(String.class);
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(json != null ? json : "{}");
        } catch (HttpClientErrorException | HttpServerErrorException e) {
            return ResponseEntity.status(e.getStatusCode()).body(e.getResponseBodyAsString());
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of("error", "ERROR_DELETE_EXCHANGE", "message", e.getMessage()));
        }
    }

    // ==========================================
    // BINDINGS
    // ==========================================

    @PostMapping("/bindings")
    public ResponseEntity<?> createBinding(@RequestBody Map<String, Object> body) {
        try {
            String json = restClient.post()
                    .uri(adminServiceUrl + "/api/bindings")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .body(String.class);
            return ResponseEntity.status(HttpStatus.CREATED).contentType(MediaType.APPLICATION_JSON).body(json);
        } catch (HttpClientErrorException | HttpServerErrorException e) {
            return ResponseEntity.status(e.getStatusCode()).body(e.getResponseBodyAsString());
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of("error", "ERROR_CREATE_BINDING", "message", e.getMessage()));
        }
    }

    @DeleteMapping("/bindings")
    public ResponseEntity<?> removeBinding(@RequestBody Map<String, Object> body) {
        try {
            String json = restClient.method(org.springframework.http.HttpMethod.DELETE)
                    .uri(adminServiceUrl + "/api/bindings")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .body(String.class);
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(json != null ? json : "{}");
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of("error", "ERROR_REMOVE_BINDING", "message", e.getMessage()));
        }
    }

    // ==========================================
    // ADMINISTRACIÓN DE APACHE KAFKA (STREAMING)
    // ==========================================

    @GetMapping("/kafka/topics")
    public ResponseEntity<?> getKafkaTopics() {
        try {
            String json = restClient.get()
                    .uri(adminServiceUrl + "/api/admin/kafka/topics")
                    .retrieve()
                    .body(String.class);
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(json != null ? json : "[]");
        } catch (HttpClientErrorException | HttpServerErrorException e) {
            return ResponseEntity.status(e.getStatusCode()).body(e.getResponseBodyAsString());
        } catch (Exception e) {
            log.error("Error consultando tópicos Kafka: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "KAFKA_UNAVAILABLE", "message", e.getMessage()));
        }
    }

    @PostMapping("/kafka/topics")
    public ResponseEntity<?> createKafkaTopic(@RequestBody Map<String, Object> body) {
        try {
            String json = restClient.post()
                    .uri(adminServiceUrl + "/api/admin/kafka/topics")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .body(String.class);
            return ResponseEntity.status(HttpStatus.CREATED).contentType(MediaType.APPLICATION_JSON).body(json);
        } catch (HttpClientErrorException | HttpServerErrorException e) {
            return ResponseEntity.status(e.getStatusCode()).contentType(MediaType.APPLICATION_JSON).body(e.getResponseBodyAsString());
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of("error", "ERROR_CREATE_KAFKA_TOPIC", "message", e.getMessage()));
        }
    }

    @DeleteMapping("/kafka/topics/{name}")
    public ResponseEntity<?> deleteKafkaTopic(@PathVariable String name) {
        try {
            String json = restClient.delete()
                    .uri(adminServiceUrl + "/api/admin/kafka/topics/" + name)
                    .retrieve()
                    .body(String.class);
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(json != null ? json : "{}");
        } catch (HttpClientErrorException | HttpServerErrorException e) {
            return ResponseEntity.status(e.getStatusCode()).body(e.getResponseBodyAsString());
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of("error", "ERROR_DELETE_KAFKA_TOPIC", "message", e.getMessage()));
        }
    }

    @GetMapping("/kafka/consumer-groups")
    public ResponseEntity<?> getKafkaConsumerGroups() {
        try {
            String json = restClient.get()
                    .uri(adminServiceUrl + "/api/admin/kafka/consumer-groups")
                    .retrieve()
                    .body(String.class);
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(json != null ? json : "[]");
        } catch (HttpClientErrorException | HttpServerErrorException e) {
            return ResponseEntity.status(e.getStatusCode()).body(e.getResponseBodyAsString());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "KAFKA_UNAVAILABLE", "message", e.getMessage()));
        }
    }

    @GetMapping("/kafka/cluster-info")
    public ResponseEntity<?> getKafkaClusterInfo() {
        try {
            String json = restClient.get()
                    .uri(adminServiceUrl + "/api/admin/kafka/cluster-info")
                    .retrieve()
                    .body(String.class);
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(json != null ? json : "{}");
        } catch (HttpClientErrorException | HttpServerErrorException e) {
            return ResponseEntity.status(e.getStatusCode()).body(e.getResponseBodyAsString());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "KAFKA_UNAVAILABLE", "message", e.getMessage()));
        }
    }

    @GetMapping("/kafka/streaming-metrics")
    public ResponseEntity<?> getKafkaStreamingMetrics() {
        try {
            String json = restClient.get()
                    .uri(requestsServiceUrl + "/api/requests/streaming/metrics")
                    .retrieve()
                    .body(String.class);
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(json != null ? json : "{}");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "STREAMING_UNAVAILABLE", "message", e.getMessage()));
        }
    }

    // ==========================================
    // PRUEBAS DE MENSAJERÍA Y DEAD LETTER QUEUE (DLQ)
    // ==========================================

    @GetMapping("/dlq/events")
    public ResponseEntity<?> getDlqEvents() {
        try {
            String json = restClient.get()
                    .uri(requestsServiceUrl + "/api/requests/dlq/events")
                    .retrieve()
                    .body(String.class);
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(json != null ? json : "[]");
        } catch (Exception e) {
            return ResponseEntity.ok("[]");
        }
    }

    @PostMapping("/test/simulate-error")
    public ResponseEntity<?> simulateDlqError(@RequestParam(defaultValue = "BD-TEST-DLQ-1001") String trackingNumber) {
        try {
            String json = restClient.post()
                    .uri(requestsServiceUrl + "/api/requests/test/simulate-error?trackingNumber=" + trackingNumber)
                    .retrieve()
                    .body(String.class);
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(json);
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of("error", "ERROR_SIMULATE_DLQ", "message", e.getMessage()));
        }
    }
}
