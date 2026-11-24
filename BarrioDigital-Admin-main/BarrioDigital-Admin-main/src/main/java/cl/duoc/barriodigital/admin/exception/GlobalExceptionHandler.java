package cl.duoc.barriodigital.admin.exception;

import cl.duoc.barriodigital.admin.dto.ErrorResponseDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponseDto> handleValidationErrors(MethodArgumentNotValidException ex) {
        List<ErrorResponseDto.ValidationErrorDetail> details = ex.getBindingResult().getFieldErrors().stream()
                .map(err -> ErrorResponseDto.ValidationErrorDetail.builder()
                        .field(err.getField())
                        .rejectedValue(err.getRejectedValue())
                        .reason(err.getDefaultMessage())
                        .build())
                .collect(Collectors.toList());

        ErrorResponseDto response = ErrorResponseDto.builder()
                .timestamp(LocalDateTime.now())
                .status(HttpStatus.BAD_REQUEST.value())
                .error("VALIDATION_ERROR")
                .message("Parámetros de entrada inválidos para la operación de mensajería")
                .details(details)
                .build();

        log.warn("Falla de validación en microservicio administrador: {}", details);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponseDto> handleIllegalArgument(IllegalArgumentException ex) {
        ErrorResponseDto response = ErrorResponseDto.builder()
                .timestamp(LocalDateTime.now())
                .status(HttpStatus.BAD_REQUEST.value())
                .error("ILLEGAL_ARGUMENT")
                .message(ex.getMessage())
                .build();

        log.warn("Argumento inválido: {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<ErrorResponseDto> handleIllegalState(IllegalStateException ex) {
        ErrorResponseDto response = ErrorResponseDto.builder()
                .timestamp(LocalDateTime.now())
                .status(HttpStatus.UNPROCESSABLE_ENTITY.value())
                .error("ILLEGAL_STATE")
                .message(ex.getMessage())
                .build();

        log.warn("Estado ilegal en broker: {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(response);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponseDto> handleGeneralException(Exception ex) {
        ErrorResponseDto response = ErrorResponseDto.builder()
                .timestamp(LocalDateTime.now())
                .status(HttpStatus.INTERNAL_SERVER_ERROR.value())
                .error("INTERNAL_ERROR")
                .message("Ocurrió un error al interactuar con el broker RabbitMQ: " + ex.getMessage())
                .build();

        log.error("Error inesperado en ms-barriodigital-admin", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
}
