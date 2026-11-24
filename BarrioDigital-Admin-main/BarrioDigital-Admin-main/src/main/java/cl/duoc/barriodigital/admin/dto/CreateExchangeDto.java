package cl.duoc.barriodigital.admin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * DTO para la creación de un Exchange en RabbitMQ.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateExchangeDto {

    @NotBlank(message = "El nombre del exchange es obligatorio y no puede estar vacío ni contener solo espacios")
    @Pattern(
            regexp = "^[a-zA-Z0-9._-]+$",
            message = "El nombre del exchange solo puede contener caracteres alfanuméricos, puntos, guiones y guiones bajos"
    )
    private String name;

    @NotBlank(message = "El tipo de exchange es obligatorio (DIRECT, TOPIC, FANOUT, HEADERS)")
    @Pattern(
            regexp = "(?i)^(DIRECT|TOPIC|FANOUT|HEADERS)$",
            message = "Tipo de exchange inválido. Los tipos permitidos son: DIRECT, TOPIC, FANOUT, HEADERS"
    )
    private String type;

    @Builder.Default
    private boolean durable = true;

    @Builder.Default
    private boolean autoDelete = false;

    private Map<String, Object> arguments;
}
