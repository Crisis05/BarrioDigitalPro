package cl.duoc.barriodigital.requests.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DlqAuditRecord {
    private String queue;
    private String reason;
    private Object payload;
    private LocalDateTime timestamp;
}
