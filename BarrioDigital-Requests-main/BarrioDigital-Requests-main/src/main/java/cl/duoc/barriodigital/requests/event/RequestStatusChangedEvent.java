package cl.duoc.barriodigital.requests.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RequestStatusChangedEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long requestId;
    private String trackingNumber;
    private String citizenEmail;
    private String citizenName;
    private String previousStatus;
    private String newStatus;
    private String performedBy;
    private String userRole;
    private String comments;
    private LocalDateTime timestamp;
}
