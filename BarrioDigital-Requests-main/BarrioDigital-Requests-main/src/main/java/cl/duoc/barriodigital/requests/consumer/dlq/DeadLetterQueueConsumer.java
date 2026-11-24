package cl.duoc.barriodigital.requests.consumer.dlq;

import cl.duoc.barriodigital.requests.event.DlqAuditRecord;
import com.rabbitmq.client.Channel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

@Component
@ConditionalOnProperty(name = "spring.rabbitmq.host")
@Slf4j
public class DeadLetterQueueConsumer {

    @Getter
    private final List<DlqAuditRecord> recordedDlqEvents = Collections.synchronizedList(new LinkedList<>());

    @RabbitListener(queues = {
            "${barriodigital.rabbitmq.queues.notifications-dlq}",
            "${barriodigital.rabbitmq.queues.tickets-dlq}",
            "${barriodigital.rabbitmq.queues.status-dlq}"
    })
    public void processDeadLetterMessage(
            Message message,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag,
            @Header(value = AmqpHeaders.CONSUMER_QUEUE, required = false) String queueName
    ) throws IOException {
        String body = new String(message.getBody(), StandardCharsets.UTF_8);
        Object deathHeader = message.getMessageProperties().getHeaders().get("x-death");

        log.warn("ALERTA DLQ - MENSAJE NO PROCESADO: Cola={}, DeathHeader={}, Body={}", queueName, deathHeader, body);

        DlqAuditRecord record = DlqAuditRecord.builder()
                .queue(queueName != null ? queueName : "DLQ")
                .reason(deathHeader != null ? deathHeader.toString() : "Mensaje rechazado con basicNack(requeue=false)")
                .payload(body)
                .timestamp(LocalDateTime.now())
                .build();

        recordedDlqEvents.add(0, record);
        if (recordedDlqEvents.size() > 50) {
            recordedDlqEvents.remove(recordedDlqEvents.size() - 1);
        }
        channel.basicAck(deliveryTag, false);
    }
}