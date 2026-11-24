package cl.duoc.barriodigital.admin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * DTO para la creación de una cola en RabbitMQ con validación estricta de parámetros.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateQueueDto {

    @NotBlank(message = "El nombre de la cola es obligatorio y no puede estar vacío ni contener solo espacios")
    @Pattern(
            regexp = "^[a-zA-Z0-9._-]+$",
            message = "El nombre de la cola solo puede contener caracteres alfanuméricos, puntos, guiones y guiones bajos (sin espacios ni caracteres especiales)"
    )
    private String name;

    @Builder.Default
    private boolean durable = true;

    @Builder.Default
    private boolean exclusive = false;

    @Builder.Default
    private boolean autoDelete = false;

    private String deadLetterExchange;

    private String deadLetterRoutingKey;

    @Positive(message = "El tiempo de vida (TTL) del mensaje debe ser un número positivo en milisegundos")
    private Integer messageTtl;

    private Map<String, Object> extraArguments;
}
