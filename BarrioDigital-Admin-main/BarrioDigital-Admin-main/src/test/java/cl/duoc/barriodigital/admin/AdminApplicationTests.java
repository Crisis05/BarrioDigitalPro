package cl.duoc.barriodigital.admin;

import cl.duoc.barriodigital.admin.dto.CreateKafkaTopicDto;
import cl.duoc.barriodigital.admin.dto.CreateQueueDto;
import cl.duoc.barriodigital.admin.service.RabbitAdminService;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class AdminApplicationTests {

    @MockBean
    private RabbitAdmin rabbitAdmin;

    @MockBean
    private RabbitTemplate rabbitTemplate;

    @MockBean
    private AdminClient adminClient;

    @Autowired
    private RabbitAdminService rabbitAdminService;

    @Test
    void contextLoads() {
        assertNotNull(rabbitAdminService);
    }

    @Test
    void testCreateQueueValidation() {
        CreateQueueDto dto = CreateQueueDto.builder()
                .name("test.unit.queue")
                .durable(true)
                .build();
        assertNotNull(dto.getName());
        assertEquals("test.unit.queue", dto.getName());
    }

    @Test
    void testCreateKafkaTopicValidation() {
        CreateKafkaTopicDto dto = CreateKafkaTopicDto.builder()
                .name("test.kafka.topic")
                .partitions(3)
                .replicationFactor((short) 1)
                .retentionMs(604800000L)
                .build();
        assertEquals("test.kafka.topic", dto.getName());
        assertEquals(3, dto.getPartitions());
        assertEquals((short) 1, dto.getReplicationFactor());
        assertEquals(604800000L, dto.getRetentionMs());
    }
}
