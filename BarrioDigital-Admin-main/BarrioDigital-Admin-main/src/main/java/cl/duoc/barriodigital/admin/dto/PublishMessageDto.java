package cl.duoc.barriodigital.admin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * DTO para publicar un mensaje de prueba al broker RabbitMQ.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PublishMessageDto {

    private String exchange;

    @NotBlank(message = "La clave de enrutamiento (routing key) es obligatoria")
    private String routingKey;

    @NotNull(message = "El contenido del mensaje (payload) no puede ser nulo")
    private Object payload;

    private Map<String, Object> headers;
}
