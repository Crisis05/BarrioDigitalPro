package cl.duoc.barriodigital.admin.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuración del AdminClient de Apache Kafka para operaciones de gestión:
 * creación, eliminación e inspección de tópicos y consumer groups.
 */
@Configuration
public class KafkaAdminConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Bean(destroyMethod = "close")
    public AdminClient kafkaAdminClient() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        configs.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000);
        return AdminClient.create(configs);
    }
}
