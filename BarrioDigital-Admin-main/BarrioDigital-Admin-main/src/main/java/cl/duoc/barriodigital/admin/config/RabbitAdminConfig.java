package cl.duoc.barriodigital.admin.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuración de infraestructura Spring AMQP para el microservicio administrador.
 * Declara el bean RabbitAdmin que gestiona de manera declarativa y dinámica
 * colas, exchanges y bindings.
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
public class RabbitAdminConfig {

    private final RabbitProperties rabbitProperties;

    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        RabbitAdmin admin = new RabbitAdmin(connectionFactory);
        admin.setAutoStartup(false);
        log.info("RabbitAdmin inicializado correctamente para ms-barriodigital-admin.");
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
    // Declaración de Exchanges Base
    // ==========================================

    @Bean
    public DirectExchange mainExchange() {
        return ExchangeBuilder.directExchange(rabbitProperties.getExchanges().getMain())
                .durable(true)
                .build();
    }

    @Bean
    public DirectExchange deadLetterExchange() {
        return ExchangeBuilder.directExchange(rabbitProperties.getExchanges().getDlx())
                .durable(true)
                .build();
    }

    // ==========================================
    // Declaración de Colas Principales con DLQ
    // ==========================================

    @Bean
    public Queue notificationsQueue() {
        return QueueBuilder.durable(rabbitProperties.getQueues().getNotifications())
                .withArgument("x-dead-letter-exchange", rabbitProperties.getExchanges().getDlx())
                .withArgument("x-dead-letter-routing-key", rabbitProperties.getRoutingKeys().getDlqNotifications())
                .build();
    }

    @Bean
    public Queue notificationsDlq() {
        return QueueBuilder.durable(rabbitProperties.getQueues().getNotificationsDlq())
                .build();
    }

    @Bean
    public Binding notificationsBinding(Queue notificationsQueue, DirectExchange mainExchange) {
        return BindingBuilder.bind(notificationsQueue)
                .to(mainExchange)
                .with(rabbitProperties.getRoutingKeys().getNotificationCreated());
    }

    @Bean
    public Binding notificationsDlqBinding(Queue notificationsDlq, DirectExchange deadLetterExchange) {
        return BindingBuilder.bind(notificationsDlq)
                .to(deadLetterExchange)
                .with(rabbitProperties.getRoutingKeys().getDlqNotifications());
    }

    // ==========================================
    // Cola de Generación de Tickets Comanales
    // ==========================================

    @Bean
    public Queue ticketsQueue() {
        return QueueBuilder.durable(rabbitProperties.getQueues().getTickets())
                .withArgument("x-dead-letter-exchange", rabbitProperties.getExchanges().getDlx())
                .withArgument("x-dead-letter-routing-key", rabbitProperties.getRoutingKeys().getDlqTickets())
                .build();
    }

    @Bean
    public Queue ticketsDlq() {
        return QueueBuilder.durable(rabbitProperties.getQueues().getTicketsDlq())
                .build();
    }

    @Bean
    public Binding ticketsBinding(Queue ticketsQueue, DirectExchange mainExchange) {
        return BindingBuilder.bind(ticketsQueue)
                .to(mainExchange)
                .with(rabbitProperties.getRoutingKeys().getTicketGenerate());
    }

    @Bean
    public Binding ticketsDlqBinding(Queue ticketsDlq, DirectExchange deadLetterExchange) {
        return BindingBuilder.bind(ticketsDlq)
                .to(deadLetterExchange)
                .with(rabbitProperties.getRoutingKeys().getDlqTickets());
    }
}
