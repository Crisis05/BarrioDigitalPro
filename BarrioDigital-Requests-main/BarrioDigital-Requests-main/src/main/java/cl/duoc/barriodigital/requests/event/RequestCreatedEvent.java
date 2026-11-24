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
public class RequestCreatedEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long requestId;
    private String trackingNumber;
    private String citizenDni;
    private String citizenName;
    private String citizenEmail;
    private String citizenPhone;
    private Long procedureTypeId;
    private String procedureName;
    private String description;
    private String address;
    private String createdBy;
    private LocalDateTime timestamp;
    private boolean simulateError; // Permite pruebas en vivo para demostrar desvío a DLQ
}
