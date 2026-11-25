package cl.duoc.barriodigital.admin.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KafkaTopicInfoDto {
    private String name;
    private int partitions;
    private short replicationFactor;
    private boolean internal;
    private Map<String, String> configs;
}
