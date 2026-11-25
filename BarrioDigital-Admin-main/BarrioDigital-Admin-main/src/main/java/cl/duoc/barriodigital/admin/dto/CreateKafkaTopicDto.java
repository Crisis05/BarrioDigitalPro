package cl.duoc.barriodigital.admin.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateKafkaTopicDto {

    @NotBlank(message = "El nombre del tópico es obligatorio")
    private String name;

    @Builder.Default
    private int partitions = 3;

    @Builder.Default
    private short replicationFactor = 1;

    @Builder.Default
    private Long retentionMs = 604800000L; // 7 días

    @Builder.Default
    private String cleanupPolicy = "delete";
}
