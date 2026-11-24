package cl.duoc.barriodigital.admin.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * DTO para la creación de un Binding entre Exchange y Cola.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateBindingDto {

    @NotBlank(message = "El nombre de la cola destino es obligatorio")
    private String queueName;

    @NotBlank(message = "El nombre del exchange origen es obligatorio")
    private String exchangeName;

    @NotBlank(message = "La clave de enrutamiento (routing key) es obligatoria")
    private String routingKey;

    private Map<String, Object> arguments;
}
