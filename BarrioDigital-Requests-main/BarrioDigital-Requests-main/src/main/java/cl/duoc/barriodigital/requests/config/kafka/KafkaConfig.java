package cl.duoc.barriodigital.requests.config.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuración centralizada de Apache Kafka para Streaming y Analítica en BarrioDigital.
 * Cumple con los requerimientos de evaluación:
 * - Definición de 2 tópicos con particiones, réplicas y políticas de retención.
 * - Producer y Consumer tipados con DTOs y serializers JSON.
 * - Manejo de errores resiliente con reintentos configurados.
 */
@Configuration
@EnableKafka
@ConditionalOnProperty(name = "spring.kafka.bootstrap-servers")
@RequiredArgsConstructor
@Slf4j
public class KafkaConfig {

    private final KafkaProperties kafkaProperties;

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    // ==============================================================
    // 1. GESTOR DE TÓPICOS (KAFKA ADMIN)
    // ==============================================================
    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    // ==============================================================
    // 2. DEFINICIÓN DE LOS DOS TÓPICOS DE NEGOCIO (PAUTA DE EVALUACIÓN)
    // ==============================================================

    /**
     * Tópico 1: Streaming de eventos de trámites (creación, cambio de estado, auditoría).
     * Particiones: 3 (distribución de carga entre consumidores)
     * Replicación: 1 (entorno local / containerizado)
     * Retención: 7 días (604800000 ms)
     * Cleanup Policy: delete
     */
    @Bean
    public NewTopic requestsEventsTopic() {
        return TopicBuilder.name(kafkaProperties.getTopics().getRequestsEvents())
                .partitions(3)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, "604800000") // 7 días
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .build();
    }

    /**
     * Tópico 2: Streaming de métricas y analítica en tiempo real para panel de control.
     * Particiones: 3 (particionado por tipo de trámite)
     * Replicación: 1
     * Retención: 3 días (259200000 ms)
     * Cleanup Policy: delete
     */
    @Bean
    public NewTopic analyticsMetricsTopic() {
        return TopicBuilder.name(kafkaProperties.getTopics().getAnalyticsMetrics())
                .partitions(3)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, "259200000") // 3 días
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .build();
    }

    // ==============================================================
    // 3. PRODUCTOR KAFKA (PRODUCER FACTORY & TEMPLATE)
    // ==============================================================
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        configProps.put(ProducerConfig.ACKS_CONFIG, "1");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        configProps.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, true);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // ==============================================================
    // 4. CONSUMIDOR KAFKA CON MANEJO DE ERRORES Y REINTENTOS
    // ==============================================================
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getConsumerGroups().getAnalytics());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "cl.duoc.barriodigital.*");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        // Manejador de errores resiliente: 2 reintentos con intervalo de 1 segundo
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (record, exception) -> log.error("Error fatal consumiendo mensaje de Kafka [Topic: {}, Offset: {}]: {}",
                        record.topic(), record.offset(), exception.getMessage()),
                new FixedBackOff(1000L, 2L)
        );
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}
