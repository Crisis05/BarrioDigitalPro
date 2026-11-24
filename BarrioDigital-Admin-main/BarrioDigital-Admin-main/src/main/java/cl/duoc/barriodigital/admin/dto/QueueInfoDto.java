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
public class QueueInfoDto {
    private String name;
    private int messageCount;
    private int consumerCount;
    private boolean durable;
    private boolean exclusive;
    private boolean autoDelete;
    private Map<String, Object> arguments;
    private String status;
}
