package cl.duoc.barriodigital.admin.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/**
 * Clase de configuración centralizada que mapea los nombres de colas,
 * exchanges y routing keys definidos en application.yml.
 * Evita valores sueltos (hardcoded strings) en el código.
 */
@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "barriodigital.rabbitmq")
public class RabbitProperties {

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
        private String catalogQuota;
        private String catalogQuotaDlq;
    }

    @Getter
    @Setter
    public static class RoutingKeys {
        private String notificationCreated;
        private String notificationUpdated;
        private String ticketGenerate;
        private String statusChanged;
        private String catalogQuotaDecrement;
        private String dlqNotifications;
        private String dlqTickets;
        private String dlqStatus;
        private String dlqCatalogQuota;
    }
}
