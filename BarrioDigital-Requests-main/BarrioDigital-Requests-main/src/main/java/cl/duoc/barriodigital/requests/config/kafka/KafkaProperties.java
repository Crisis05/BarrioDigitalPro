package cl.duoc.barriodigital.requests.config.kafka;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "barriodigital.kafka")
@Getter
@Setter
public class KafkaProperties {

    private Topics topics = new Topics();
    private ConsumerGroups consumerGroups = new ConsumerGroups();

    @Getter
    @Setter
    public static class Topics {
        private String requestsEvents = "barriodigital.requests.events";
        private String analyticsMetrics = "barriodigital.analytics.metrics";
    }

    @Getter
    @Setter
    public static class ConsumerGroups {
        private String analytics = "barriodigital-analytics-group";
        private String audit = "barriodigital-audit-group";
    }
}
