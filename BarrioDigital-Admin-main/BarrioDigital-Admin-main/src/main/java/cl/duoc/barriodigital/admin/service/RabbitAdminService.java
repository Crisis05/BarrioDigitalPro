package cl.duoc.barriodigital.admin.service;

import cl.duoc.barriodigital.admin.dto.*;

import java.util.List;

/**
 * Servicio que encapsula toda la interacción y operaciones administrativas sobre RabbitMQ.
 * Proporciona métodos de alto nivel para gestionar colas, exchanges, bindings y pruebas de mensajería,
 * aislando completamente la infraestructura AMQP de los controladores REST.
 */
public interface RabbitAdminService {

    QueueInfoDto createQueue(CreateQueueDto dto);

    boolean deleteQueue(String queueName);

    boolean purgeQueue(String queueName);

    QueueInfoDto getQueueInfo(String queueName);

    List<QueueInfoDto> listConfiguredQueues();

    void createExchange(CreateExchangeDto dto);

    boolean deleteExchange(String exchangeName);

    void createBinding(CreateBindingDto dto);

    boolean removeBinding(CreateBindingDto dto);

    void publishMessage(PublishMessageDto dto);
}
