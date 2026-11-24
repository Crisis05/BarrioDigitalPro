package cl.duoc.barriodigital.requests.publisher;

import cl.duoc.barriodigital.requests.event.RequestCreatedEvent;
import cl.duoc.barriodigital.requests.event.RequestStatusChangedEvent;

/**
 * Interfaz desacoplada para publicación de eventos de dominio sobre trámites.
 * Aísla completamente la lógica de negocio del servicio respecto al broker RabbitMQ.
 */
public interface RequestEventPublisher {

    void publishRequestCreated(RequestCreatedEvent event);

    void publishStatusChanged(RequestStatusChangedEvent event);
}
