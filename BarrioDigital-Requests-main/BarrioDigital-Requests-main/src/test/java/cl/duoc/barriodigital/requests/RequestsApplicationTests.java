package cl.duoc.barriodigital.requests;

import cl.duoc.barriodigital.requests.dto.CreateRequestDto;
import cl.duoc.barriodigital.requests.dto.UpdateStatusDto;
import cl.duoc.barriodigital.requests.model.Request;
import cl.duoc.barriodigital.requests.model.RequestAuditLog;
import cl.duoc.barriodigital.requests.model.RequestStatus;
import cl.duoc.barriodigital.requests.publisher.RequestEventPublisher;
import cl.duoc.barriodigital.requests.service.RequestService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.TestPropertySource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@TestPropertySource(properties = {
    "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration"
})
class RequestsApplicationTests {

    @MockBean
    private RequestEventPublisher eventPublisher;

    @Autowired
    private RequestService requestService;

    @Test
    void contextLoads() {
        assertNotNull(requestService);
    }

    @Test
    void testCreateRequestAndInitialStatus() {
        CreateRequestDto dto = CreateRequestDto.builder()
                .citizenDni("19.876.543-2")
                .citizenName("Ana Morales")
                .citizenEmail("ana.morales@vecinos.cl")
                .citizenPhone("+56988776655")
                .procedureTypeId(1L)
                .procedureName("Reparación de Luminarias Públicas")
                .description("Luz rota en plaza central")
                .address("Plaza Central sector B")
                .createdBy("Ana Morales")
                .build();

        Request created = requestService.createRequest(dto);
        assertNotNull(created.getId());
        assertNotNull(created.getTrackingNumber());
        assertEquals(RequestStatus.INGRESADO, created.getStatus());

        List<RequestAuditLog> timeline = requestService.getTimeline(created.getId());
        assertFalse(timeline.isEmpty(), "Debe existir registro en la línea de tiempo de auditoría");
        assertEquals("CREACION_SOLICITUD", timeline.get(0).getAction());
    }

    @Test
    void testBusinessRule_CannotPassToEnTerrenoWithoutAdmitir() {
        CreateRequestDto dto = CreateRequestDto.builder()
                .citizenDni("12.345.678-9")
                .citizenName("Carlos Rojas")
                .citizenEmail("carlos.rojas@vecinos.cl")
                .procedureTypeId(2L)
                .procedureName("Bacheo y Reparación de Calzadas")
                .description("Bache en calle principal")
                .address("Calle Principal 500")
                .build();

        Request created = requestService.createRequest(dto);
        assertEquals(RequestStatus.INGRESADO, created.getStatus());

        UpdateStatusDto directToField = UpdateStatusDto.builder()
                .status(RequestStatus.EN_TERRENO)
                .assignedCrew("Cuadrilla 1")
                .performedBy("funcionario@barriodigital.cl")
                .build();

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            requestService.updateRequestStatus(created.getId(), directToField);
        });

        assertTrue(exception.getMessage().contains("No se puede pasar a EN_TERRENO sin antes haber sido ADMITIDO"),
                "Debe lanzar la excepción de negocio exigida por la pauta");
    }

    @Test
    void testProperFlow_AdmitirThenEnTerreno() {
        CreateRequestDto dto = CreateRequestDto.builder()
                .citizenDni("15.111.222-3")
                .citizenName("Patricia Silva")
                .procedureTypeId(3L)
                .procedureName("Poda de Árboles en Vía Pública")
                .description("Ramas tocando cables")
                .address("Pasaje Los Tilos 45")
                .build();

        Request created = requestService.createRequest(dto);

        UpdateStatusDto admitirDto = UpdateStatusDto.builder()
                .status(RequestStatus.ADMITIDO)
                .performedBy("funcionario@barriodigital.cl")
                .userRole("Funcionario")
                .comments("Admitido y validado")
                .build();
        Request admitted = requestService.updateRequestStatus(created.getId(), admitirDto);
        assertEquals(RequestStatus.ADMITIDO, admitted.getStatus());

        UpdateStatusDto terrenoDto = UpdateStatusDto.builder()
                .status(RequestStatus.EN_TERRENO)
                .assignedCrew("Cuadrilla Aseo 2")
                .performedBy("funcionario@barriodigital.cl")
                .userRole("Funcionario")
                .comments("Despachado a terreno")
                .build();
        Request inField = requestService.updateRequestStatus(created.getId(), terrenoDto);
        assertEquals(RequestStatus.EN_TERRENO, inField.getStatus());
        assertEquals("Cuadrilla Aseo 2", inField.getAssignedCrew());
    }
}
