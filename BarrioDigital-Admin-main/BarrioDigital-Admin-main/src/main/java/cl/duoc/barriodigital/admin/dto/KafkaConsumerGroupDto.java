package cl.duoc.barriodigital.admin.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KafkaConsumerGroupDto {
    private String groupId;
    private String state;
    private boolean isSimpleConsumerGroup;
}
