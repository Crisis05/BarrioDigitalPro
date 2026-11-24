package cl.duoc.barriodigital.admin.service.impl;

import cl.duoc.barriodigital.admin.config.RabbitProperties;
import cl.duoc.barriodigital.admin.dto.*;
import cl.duoc.barriodigital.admin.service.RabbitAdminService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class RabbitAdminServiceImpl implements RabbitAdminService {

    private final RabbitAdmin rabbitAdmin;
    private final RabbitTemplate rabbitTemplate;
    private final RabbitProperties rabbitProperties;

    @Override
    public QueueInfoDto createQueue(CreateQueueDto dto) {
        log.info("Creando/declarando cola RabbitMQ: {}", dto.getName());

        QueueBuilder builder = dto.isDurable()
                ? QueueBuilder.durable(dto.getName())
                : QueueBuilder.nonDurable(dto.getName());

        if (dto.isExclusive()) {
            builder.exclusive();
        }
        if (dto.isAutoDelete()) {
            builder.autoDelete();
        }

        // Configuración de Dead Letter Queue (DLQ)
        if (dto.getDeadLetterExchange() != null && !dto.getDeadLetterExchange().isBlank()) {
            builder.withArgument("x-dead-letter-exchange", dto.getDeadLetterExchange());
        }
        if (dto.getDeadLetterRoutingKey() != null && !dto.getDeadLetterRoutingKey().isBlank()) {
            builder.withArgument("x-dead-letter-routing-key", dto.getDeadLetterRoutingKey());
        }

        // Configuración de Time To Live (TTL)
        if (dto.getMessageTtl() != null && dto.getMessageTtl() > 0) {
            builder.withArgument("x-message-ttl", dto.getMessageTtl());
        }

        if (dto.getExtraArguments() != null) {
            dto.getExtraArguments().forEach(builder::withArgument);
        }

        org.springframework.amqp.core.Queue queue = builder.build();
        String declaredName = rabbitAdmin.declareQueue(queue);
        log.info("Cola '{}' declarada exitosamente en el broker RabbitMQ.", declaredName);

        return getQueueInfo(dto.getName());
    }

    @Override
    public boolean deleteQueue(String queueName) {
        log.info("Eliminando cola RabbitMQ: {}", queueName);
        boolean deleted = rabbitAdmin.deleteQueue(queueName);
        if (deleted) {
            log.info("Cola '{}' eliminada con éxito.", queueName);
        } else {
            log.warn("No se pudo eliminar la cola '{}' (posiblemente no existía).", queueName);
        }
        return deleted;
    }

    @Override
    public boolean purgeQueue(String queueName) {
        log.info("Purgando mensajes de la cola: {}", queueName);
        rabbitAdmin.purgeQueue(queueName, false);
        log.info("Cola '{}' purgada exitosamente.", queueName);
        return true;
    }

    @Override
    public QueueInfoDto getQueueInfo(String queueName) {
        Properties props = rabbitAdmin.getQueueProperties(queueName);
        if (props == null) {
            return QueueInfoDto.builder()
                    .name(queueName)
                    .messageCount(0)
                    .consumerCount(0)
                    .status("INACTIVE_OR_EMPTY")
                    .build();
        }

        int messageCount = (int) props.getOrDefault(RabbitAdmin.QUEUE_MESSAGE_COUNT, 0);
        int consumerCount = (int) props.getOrDefault(RabbitAdmin.QUEUE_CONSUMER_COUNT, 0);

        Map<String, Object> arguments = new HashMap<>();
        props.forEach((k, v) -> arguments.put(k.toString(), v));

        return QueueInfoDto.builder()
                .name(queueName)
                .messageCount(messageCount)
                .consumerCount(consumerCount)
                .arguments(arguments)
                .status("ACTIVE")
                .build();
    }

    @Override
    public List<QueueInfoDto> listConfiguredQueues() {
        List<String> knownQueues = List.of(
                rabbitProperties.getQueues().getNotifications(),
                rabbitProperties.getQueues().getNotificationsDlq(),
                rabbitProperties.getQueues().getTickets(),
                rabbitProperties.getQueues().getTicketsDlq(),
                rabbitProperties.getQueues().getStatus(),
                rabbitProperties.getQueues().getStatusDlq(),
                rabbitProperties.getQueues().getCatalogQuota(),
                rabbitProperties.getQueues().getCatalogQuotaDlq()
        );

        List<QueueInfoDto> result = new ArrayList<>();
        for (String q : knownQueues) {
            if (q != null && !q.isBlank()) {
                result.add(getQueueInfo(q));
            }
        }
        return result;
    }

    @Override
    public void createExchange(CreateExchangeDto dto) {
        log.info("Creando exchange '{}' de tipo: {}", dto.getName(), dto.getType());
        Exchange exchange;
        String typeUpper = dto.getType().trim().toUpperCase();

        switch (typeUpper) {
            case "DIRECT":
                exchange = ExchangeBuilder.directExchange(dto.getName())
                        .durable(dto.isDurable())
                        .build();
                break;
            case "TOPIC":
                exchange = ExchangeBuilder.topicExchange(dto.getName())
                        .durable(dto.isDurable())
                        .build();
                break;
            case "FANOUT":
                exchange = ExchangeBuilder.fanoutExchange(dto.getName())
                        .durable(dto.isDurable())
                        .build();
                break;
            case "HEADERS":
                exchange = ExchangeBuilder.headersExchange(dto.getName())
                        .durable(dto.isDurable())
                        .build();
                break;
            default:
                throw new IllegalArgumentException("Tipo de exchange no soportado: " + dto.getType());
        }

        rabbitAdmin.declareExchange(exchange);
        log.info("Exchange '{}' declarado exitosamente.", dto.getName());
    }

    @Override
    public boolean deleteExchange(String exchangeName) {
        log.info("Eliminando exchange: {}", exchangeName);
        boolean deleted = rabbitAdmin.deleteExchange(exchangeName);
        if (deleted) {
            log.info("Exchange '{}' eliminado con éxito.", exchangeName);
        } else {
            log.warn("No se pudo eliminar el exchange '{}'.", exchangeName);
        }
        return deleted;
    }

    @Override
    public void createBinding(CreateBindingDto dto) {
        log.info("Creando binding: Cola '{}' -> Exchange '{}' [RoutingKey: '{}']",
                dto.getQueueName(), dto.getExchangeName(), dto.getRoutingKey());

        Binding binding = new Binding(
                dto.getQueueName(),
                Binding.DestinationType.QUEUE,
                dto.getExchangeName(),
                dto.getRoutingKey(),
                dto.getArguments()
        );

        rabbitAdmin.declareBinding(binding);
        log.info("Binding creado exitosamente.");
    }

    @Override
    public boolean removeBinding(CreateBindingDto dto) {
        log.info("Removiendo binding: Cola '{}' -> Exchange '{}' [RoutingKey: '{}']",
                dto.getQueueName(), dto.getExchangeName(), dto.getRoutingKey());

        Binding binding = new Binding(
                dto.getQueueName(),
                Binding.DestinationType.QUEUE,
                dto.getExchangeName(),
                dto.getRoutingKey(),
                dto.getArguments()
        );

        rabbitAdmin.removeBinding(binding);
        log.info("Binding removido exitosamente.");
        return true;
    }

    @Override
    public void publishMessage(PublishMessageDto dto) {
        String exchange = (dto.getExchange() != null) ? dto.getExchange() : "";
        log.info("Publicando mensaje a exchange '{}' con routingKey '{}'", exchange, dto.getRoutingKey());

        rabbitTemplate.convertAndSend(exchange, dto.getRoutingKey(), dto.getPayload(), message -> {
            if (dto.getHeaders() != null) {
                dto.getHeaders().forEach((k, v) -> message.getMessageProperties().setHeader(k, v));
            }
            return message;
        });

        log.info("Mensaje publicado exitosamente en RabbitMQ.");
    }
}
