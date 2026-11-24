package cl.duoc.barriodigital.admin;

import cl.duoc.barriodigital.admin.dto.CreateQueueDto;
import cl.duoc.barriodigital.admin.service.RabbitAdminService;
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
}
