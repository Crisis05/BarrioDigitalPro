package cl.duoc.barriodigital.requests.config.rabbit;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuración de infraestructura RabbitMQ para el microservicio ms-barriodigital-requests.
 * Declara de forma centralizada beans de Exchange, Queue, DLQ y Binding para cada caso de uso:
 * 1. Notificaciones por correo/SMS al vecino
 * 2. Generación asíncrona de comprobantes/tickets comunales
 * 3. Notificación de cambios de estado comunales
 *
 * Esta configuración solo se activa cuando spring.rabbitmq.host está definido
 * (es decir, en entornos reales). En tests, se excluye junto con RabbitAutoConfiguration.
 */
@Configuration
@ConditionalOnProperty(name = "spring.rabbitmq.host")
@RequiredArgsConstructor
@Slf4j
public class RabbitMQConfig {

    private final RabbitMQProperties properties;

    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        RabbitAdmin admin = new RabbitAdmin(connectionFactory);
        admin.setAutoStartup(false);
        return admin;
    }

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory, MessageConverter jsonMessageConverter) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(jsonMessageConverter);
        return template;
    }

    // ==========================================
    // EXCHANGES (Principal y Dead Letter Exchange)
    // ==========================================

    @Bean
    public DirectExchange mainExchange() {
        return ExchangeBuilder.directExchange(properties.getExchanges().getMain())
                .durable(true)
                .build();
    }

    @Bean
    public DirectExchange deadLetterExchange() {
        return ExchangeBuilder.directExchange(properties.getExchanges().getDlx())
                .durable(true)
                .build();
    }

    // ==========================================
    // CASO DE USO 1: NOTIFICACIONES AL VECINO
    // ==========================================

    @Bean
    public Queue notificationsQueue() {
        return QueueBuilder.durable(properties.getQueues().getNotifications())
                .withArgument("x-dead-letter-exchange", properties.getExchanges().getDlx())
                .withArgument("x-dead-letter-routing-key", properties.getRoutingKeys().getDlqNotifications())
                .build();
    }

    @Bean
    public Queue notificationsDlq() {
        return QueueBuilder.durable(properties.getQueues().getNotificationsDlq())
                .build();
    }

    @Bean
    public Binding notificationsBinding(Queue notificationsQueue, DirectExchange mainExchange) {
        return BindingBuilder.bind(notificationsQueue)
                .to(mainExchange)
                .with(properties.getRoutingKeys().getNotificationCreated());
    }

    @Bean
    public Binding notificationsDlqBinding(Queue notificationsDlq, DirectExchange deadLetterExchange) {
        return BindingBuilder.bind(notificationsDlq)
                .to(deadLetterExchange)
                .with(properties.getRoutingKeys().getDlqNotifications());
    }

    // ==========================================
    // CASO DE USO 2: GENERACIÓN DE TICKETS / COMPROBANTES
    // ==========================================

    @Bean
    public Queue ticketsQueue() {
        return QueueBuilder.durable(properties.getQueues().getTickets())
                .withArgument("x-dead-letter-exchange", properties.getExchanges().getDlx())
                .withArgument("x-dead-letter-routing-key", properties.getRoutingKeys().getDlqTickets())
                .build();
    }

    @Bean
    public Queue ticketsDlq() {
        return QueueBuilder.durable(properties.getQueues().getTicketsDlq())
                .build();
    }

    @Bean
    public Binding ticketsBinding(Queue ticketsQueue, DirectExchange mainExchange) {
        return BindingBuilder.bind(ticketsQueue)
                .to(mainExchange)
                .with(properties.getRoutingKeys().getTicketGenerate());
    }

    @Bean
    public Binding ticketsDlqBinding(Queue ticketsDlq, DirectExchange deadLetterExchange) {
        return BindingBuilder.bind(ticketsDlq)
                .to(deadLetterExchange)
                .with(properties.getRoutingKeys().getDlqTickets());
    }

    // ==========================================
    // CASO DE USO 3: NOTIFICACIÓN CAMBIOS DE ESTADO
    // ==========================================

    @Bean
    public Queue statusQueue() {
        return QueueBuilder.durable(properties.getQueues().getStatus())
                .withArgument("x-dead-letter-exchange", properties.getExchanges().getDlx())
                .withArgument("x-dead-letter-routing-key", properties.getRoutingKeys().getDlqStatus())
                .build();
    }

    @Bean
    public Queue statusDlq() {
        return QueueBuilder.durable(properties.getQueues().getStatusDlq())
                .build();
    }

    @Bean
    public Binding statusBinding(Queue statusQueue, DirectExchange mainExchange) {
        return BindingBuilder.bind(statusQueue)
                .to(mainExchange)
                .with(properties.getRoutingKeys().getStatusChanged());
    }

    @Bean
    public Binding statusDlqBinding(Queue statusDlq, DirectExchange deadLetterExchange) {
        return BindingBuilder.bind(statusDlq)
                .to(deadLetterExchange)
                .with(properties.getRoutingKeys().getDlqStatus());
    }
}
