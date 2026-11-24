package cl.duoc.barriodigital.requests.config.rabbit;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Mapeo centralizado de propiedades de mensajería RabbitMQ.
 * Centraliza nombres de colas, exchanges y routing keys para evitar strings hardcodeados.
 */
@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "barriodigital.rabbitmq")
public class RabbitMQProperties {

    private Exchanges exchanges = new Exchanges();
    private Queues queues = new Queues();
    private RoutingKeys routingKeys = new RoutingKeys();

    @Getter
    @Setter
    public static class Exchanges {
        private String main;
        private String dlx;
    }

    @Getter
    @Setter
    public static class Queues {
        private String notifications;
        private String notificationsDlq;
        private String tickets;
        private String ticketsDlq;
        private String status;
        private String statusDlq;
    }

    @Getter
    @Setter
    public static class RoutingKeys {
        private String notificationCreated;
        private String notificationUpdated;
        private String ticketGenerate;
        private String statusChanged;
        private String dlqNotifications;
        private String dlqTickets;
        private String dlqStatus;
    }
}
