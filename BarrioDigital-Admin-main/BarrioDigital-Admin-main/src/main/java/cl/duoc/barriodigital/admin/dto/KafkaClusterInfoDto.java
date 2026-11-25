package cl.duoc.barriodigital.admin.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KafkaClusterInfoDto {
    private String clusterId;
    private int nodesCount;
    private List<String> nodes;
    private String controllerNode;
}
