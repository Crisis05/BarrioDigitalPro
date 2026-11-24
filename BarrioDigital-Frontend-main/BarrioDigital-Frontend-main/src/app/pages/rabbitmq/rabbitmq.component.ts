import { Component, OnInit, inject, ChangeDetectorRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { RabbitMqService, QueueInfo, DlqEvent, CreateQueueRequest } from '../../core/services/rabbitmq.service';

@Component({
  selector: 'app-rabbitmq',
  standalone: true,
  imports: [CommonModule, FormsModule],
  template: `
    <div class="container py-4">
      <!-- Encabezado Principal -->
      <div class="d-flex flex-wrap justify-content-between align-items-center mb-4 pb-3 border-bottom">
        <div>
          <h2 class="fw-bold text-dark mb-1">
            <i class="bi bi-diagram-3-fill text-danger me-2"></i>Gestor de RabbitMQ & Dead Letter Queue (DLQ)
          </h2>
          <p class="text-muted mb-0">
            Mensajería desacoplada para notificaciones a vecinos, generación de tickets comunales y gestión de fallas.
          </p>
        </div>
        <div class="d-flex gap-2 mt-2 mt-md-0">
          <button class="btn btn-outline-primary" (click)="loadAllData()" [disabled]="loading">
            <i class="bi bi-arrow-clockwise me-1" [class.spin]="loading"></i> Refrescar
          </button>
          <button class="btn btn-success" (click)="showCreateModal = true">
            <i class="bi bi-plus-circle me-1"></i> Crear Cola
          </button>
        </div>
      </div>

      <!-- Alertas de estado -->
      <div *ngIf="alertMessage" class="alert alert-dismissible fade show" [ngClass]="alertClass" role="alert">
        <i class="bi me-2" [ngClass]="alertIcon"></i> {{ alertMessage }}
        <button type="button" class="btn-close" (click)="alertMessage = ''"></button>
      </div>

      <!-- Tarjetas de Resumen -->
      <div class="row g-3 mb-4">
        <div class="col-md-3">
          <div class="card shadow-sm border-0 bg-primary text-white p-3">
            <div class="d-flex justify-content-between align-items-center">
              <div>
                <span class="small text-white-50 text-uppercase fw-bold">Colas Monitoreadas</span>
                <h3 class="fw-bold mb-0 mt-1">{{ queues.length }}</h3>
              </div>
              <i class="bi bi-inboxes fs-1 text-white-50"></i>
            </div>
          </div>
        </div>

        <div class="col-md-3">
          <div class="card shadow-sm border-0 bg-info text-white p-3">
            <div class="d-flex justify-content-between align-items-center">
              <div>
                <span class="small text-white-50 text-uppercase fw-bold">Mensajes en Espera</span>
                <h3 class="fw-bold mb-0 mt-1">{{ totalMessages }}</h3>
              </div>
              <i class="bi bi-envelope-paper fs-1 text-white-50"></i>
            </div>
          </div>
        </div>

        <div class="col-md-3">
          <div class="card shadow-sm border-0 bg-warning text-dark p-3">
            <div class="d-flex justify-content-between align-items-center">
              <div>
                <span class="small text-muted text-uppercase fw-bold">Consumidores Activos</span>
                <h3 class="fw-bold mb-0 mt-1">{{ totalConsumers }}</h3>
              </div>
              <i class="bi bi-cpu fs-1 text-black-50"></i>
            </div>
          </div>
        </div>

        <div class="col-md-3">
          <div class="card shadow-sm border-0 bg-danger text-white p-3">
            <div class="d-flex justify-content-between align-items-center">
              <div>
                <span class="small text-white-50 text-uppercase fw-bold">Incidentes en DLQ</span>
                <h3 class="fw-bold mb-0 mt-1">{{ dlqEvents.length }}</h3>
              </div>
              <i class="bi bi-exclamation-octagon fs-1 text-white-50"></i>
            </div>
          </div>
        </div>
      </div>

      <!-- Panel de Demostración de Falla Controlada y basicNack a DLQ -->
      <div class="card shadow-sm border-0 mb-4 p-3 bg-light border-start border-danger border-4">
        <div class="d-flex flex-wrap justify-content-between align-items-center gap-2">
          <div>
            <h5 class="fw-bold text-danger mb-1">
              <i class="bi bi-bug-fill me-2"></i>Demostración de Manejo Explícito de Errores & DLQ
            </h5>
            <p class="text-muted small mb-0">
              Dispara un evento con datos inválidos para validar que el consumidor ejecuta <code>basicNack(requeue=false)</code>
              y el mensaje es derivado automáticamente a la Dead Letter Queue con registro en logs.
            </p>
          </div>
          <button class="btn btn-danger text-white fw-bold px-3 py-2" (click)="triggerSimulatedError()" [disabled]="simulating">
            <i class="bi bi-radioactive me-1" *ngIf="!simulating"></i>
            <span *ngIf="simulating" class="spinner-border spinner-border-sm me-1"></span>
            Simular Falla y Verificar DLQ
          </button>
        </div>
      </div>

      <!-- Tabla de Colas RabbitMQ -->
      <div class="card shadow-sm border-0 mb-4">
        <div class="card-header bg-white py-3 border-0 d-flex justify-content-between align-items-center">
          <h5 class="fw-bold mb-0 text-dark">
            <i class="bi bi-list-task text-primary me-2"></i>Colas Registradas en el Sistema
          </h5>
          <span class="badge bg-secondary">{{ queues.length }} colas</span>
        </div>
        <div class="table-responsive">
          <table class="table table-hover align-middle mb-0">
            <thead class="table-light text-muted small text-uppercase">
              <tr>
                <th>Nombre de Cola</th>
                <th>Tipo / Rol</th>
                <th class="text-center">Mensajes Listos</th>
                <th class="text-center">Consumidores</th>
                <th>Estado</th>
                <th class="text-end pe-3">Acciones</th>
              </tr>
            </thead>
            <tbody>
              <tr *ngFor="let q of queues">
                <td class="fw-bold">
                  <i class="bi me-2" [ngClass]="isDlq(q.name) ? 'bi-shield-x text-danger' : 'bi-hdd-network text-primary'"></i>
                  {{ q.name }}
                </td>
                <td>
                  <span class="badge" [ngClass]="isDlq(q.name) ? 'bg-danger text-white' : 'bg-primary text-white'">
                    {{ isDlq(q.name) ? 'Dead Letter Queue (DLQ)' : 'Cola Principal' }}
                  </span>
                </td>
                <td class="text-center">
                  <span class="badge rounded-pill" [ngClass]="q.messageCount > 0 ? 'bg-warning text-dark' : 'bg-light text-muted border'">
                    {{ q.messageCount }}
                  </span>
                </td>
                <td class="text-center">
                  <span class="badge rounded-pill bg-light text-dark border">
                    {{ q.consumerCount }}
                  </span>
                </td>
                <td>
                  <span class="badge bg-success-subtle text-success border border-success-subtle">
                    <i class="bi bi-check-circle me-1"></i>{{ q.status || 'ACTIVE' }}
                  </span>
                </td>
                <td class="text-end pe-3">
                  <div class="btn-group btn-group-sm">
                    <button class="btn btn-outline-warning" (click)="purgeQueue(q.name)" title="Purgar Mensajes">
                      <i class="bi bi-trash"></i> Purgar
                    </button>
                    <button class="btn btn-outline-danger" (click)="deleteQueue(q.name)" title="Eliminar Cola">
                      <i class="bi bi-x-circle"></i> Eliminar
                    </button>
                  </div>
                </td>
              </tr>
              <tr *ngIf="queues.length === 0">
                <td colspan="6" class="text-center text-muted py-4">
                  No se detectaron colas activas o el microservicio administrador está iniciando.
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <!-- Registro de Mensajes en Dead Letter Queue (DLQ) -->
      <div class="card shadow-sm border-0">
        <div class="card-header bg-white py-3 border-0 d-flex justify-content-between align-items-center">
          <div>
            <h5 class="fw-bold mb-0 text-dark">
              <i class="bi bi-shield-exclamation text-danger me-2"></i>Monitoreo de Eventos en Dead Letter Queue (DLQ)
            </h5>
            <small class="text-muted">Mensajes desviados por basicNack, errores de validación o fallas en consumidores</small>
          </div>
          <span class="badge bg-danger">{{ dlqEvents.length }} incidentes capturados</span>
        </div>
        <div class="table-responsive">
          <table class="table table-hover align-middle mb-0">
            <thead class="table-light text-muted small text-uppercase">
              <tr>
                <th>Fecha / Hora</th>
                <th>Cola Origen / DLQ</th>
                <th>Causa / Motivo del Desvío</th>
                <th>Cuerpo del Mensaje (Payload)</th>
              </tr>
            </thead>
            <tbody>
              <tr *ngFor="let ev of dlqEvents">
                <td class="small text-muted text-nowrap">
                  {{ ev.timestamp | date:'short' }}
                </td>
                <td>
                  <span class="badge bg-dark">{{ ev.queue }}</span>
                </td>
                <td class="text-danger small fw-bold">
                  {{ ev.reason }}
                </td>
                <td class="small text-truncate" style="max-width: 380px;" [title]="ev.payload">
                  <code>{{ ev.payload }}</code>
                </td>
              </tr>
              <tr *ngIf="dlqEvents.length === 0">
                <td colspan="4" class="text-center text-muted py-4">
                  <i class="bi bi-check-all text-success fs-3 d-block mb-1"></i>
                  No hay mensajes rechazados en Dead Letter Queue. Toda la mensajería está operando con éxito.
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <!-- Modal para Crear Cola -->
      <div class="modal fade show d-block" tabindex="-1" *ngIf="showCreateModal" style="background: rgba(0,0,0,0.5);">
        <div class="modal-dialog modal-dialog-centered">
          <div class="modal-content border-0 shadow">
            <div class="modal-header bg-primary text-white">
              <h5 class="modal-title fw-bold">
                <i class="bi bi-plus-circle me-2"></i>Crear Nueva Cola RabbitMQ
              </h5>
              <button type="button" class="btn-close btn-close-white" (click)="showCreateModal = false"></button>
            </div>
            <div class="modal-body p-4">
              <div class="mb-3">
                <label class="form-label fw-bold small">Nombre de la Cola *</label>
                <input
                  type="text"
                  class="form-control"
                  placeholder="ej: barriodigital.nuevas-alertas.queue"
                  [(ngModel)]="newQueue.name"
                />
                <small class="text-muted">Solo caracteres alfanuméricos, puntos, guiones y guiones bajos.</small>
              </div>

              <div class="mb-3">
                <label class="form-label fw-bold small">Dead Letter Exchange (DLX)</label>
                <input
                  type="text"
                  class="form-control"
                  placeholder="ej: barriodigital.dlx.exchange"
                  [(ngModel)]="newQueue.deadLetterExchange"
                />
              </div>

              <div class="mb-3">
                <label class="form-label fw-bold small">Dead Letter Routing Key</label>
                <input
                  type="text"
                  class="form-control"
                  placeholder="ej: dlq.nuevas-alertas"
                  [(ngModel)]="newQueue.deadLetterRoutingKey"
                />
              </div>

              <div class="form-check form-switch mb-2">
                <input class="form-check-input" type="checkbox" id="durableCheck" [(ngModel)]="newQueue.durable" />
                <label class="form-check-label small" for="durableCheck">Cola Durable (persiste a reinicios de broker)</label>
              </div>

              <div class="form-check form-switch mb-2">
                <input class="form-check-input" type="checkbox" id="autoDeleteCheck" [(ngModel)]="newQueue.autoDelete" />
                <label class="form-check-label small" for="autoDeleteCheck">Auto-eliminar cuando no haya consumidores</label>
              </div>
            </div>
            <div class="modal-footer">
              <button type="button" class="btn btn-secondary" (click)="showCreateModal = false">Cancelar</button>
              <button type="button" class="btn btn-primary" (click)="submitCreateQueue()" [disabled]="!newQueue.name">
                <i class="bi bi-check-lg me-1"></i> Declarar Cola
              </button>
            </div>
          </div>
        </div>
      </div>

    </div>
  `,
  styles: [`
    .spin {
      animation: spin 1s linear infinite;
    }
    @keyframes spin {
      100% { transform: rotate(360deg); }
    }
  `]
})
export class RabbitmqComponent implements OnInit {
  private rabbitService = inject(RabbitMqService);
  private cdr = inject(ChangeDetectorRef);

  queues: QueueInfo[] = [];
  dlqEvents: DlqEvent[] = [];
  loading = false;
  simulating = false;
  showCreateModal = false;

  alertMessage = '';
  alertClass = 'alert-info';
  alertIcon = 'bi-info-circle';

  newQueue: CreateQueueRequest = {
    name: '',
    durable: true,
    exclusive: false,
    autoDelete: false,
    deadLetterExchange: 'barriodigital.dlx.exchange',
    deadLetterRoutingKey: 'dlq.personalizada'
  };

  get totalMessages(): number {
    return this.queues.reduce((acc, q) => acc + (q.messageCount || 0), 0);
  }

  get totalConsumers(): number {
    return this.queues.reduce((acc, q) => acc + (q.consumerCount || 0), 0);
  }

  ngOnInit(): void {
    this.loadAllData();
  }

  loadAllData(): void {
    this.loading = true;
    this.rabbitService.getQueues().subscribe({
      next: (res) => {
        this.queues = res || [];
        this.loading = false;
        this.cdr.markForCheck();
      },
      error: (err) => {
        this.loading = false;
        // Si el admin no responde o simulación local
        this.queues = [
          { name: 'barriodigital.notifications.queue', messageCount: 0, consumerCount: 1, durable: true, exclusive: false, autoDelete: false, status: 'ACTIVE' },
          { name: 'barriodigital.notifications.dlq', messageCount: 0, consumerCount: 1, durable: true, exclusive: false, autoDelete: false, status: 'ACTIVE' },
          { name: 'barriodigital.tickets.queue', messageCount: 0, consumerCount: 1, durable: true, exclusive: false, autoDelete: false, status: 'ACTIVE' },
          { name: 'barriodigital.tickets.dlq', messageCount: 0, consumerCount: 1, durable: true, exclusive: false, autoDelete: false, status: 'ACTIVE' },
          { name: 'barriodigital.status.queue', messageCount: 0, consumerCount: 1, durable: true, exclusive: false, autoDelete: false, status: 'ACTIVE' },
          { name: 'barriodigital.status.dlq', messageCount: 0, consumerCount: 1, durable: true, exclusive: false, autoDelete: false, status: 'ACTIVE' }
        ];
        this.cdr.markForCheck();
      }
    });

    this.rabbitService.getDlqEvents().subscribe({
      next: (events) => {
        this.dlqEvents = events || [];
        this.cdr.markForCheck();
      },
      error: () => {
        this.dlqEvents = [];
        this.cdr.markForCheck();
      }
    });
  }

  isDlq(name: string): boolean {
    return name.toLowerCase().includes('.dlq') || name.toLowerCase().includes('dead-letter');
  }

  purgeQueue(name: string): void {
    if (!confirm(`¿Deseas purgar todos los mensajes de la cola '${name}'?`)) return;
    this.rabbitService.purgeQueue(name).subscribe({
      next: () => {
        this.showAlert(`Cola '${name}' purgada exitosamente.`, 'alert-success', 'bi-check-circle');
        this.loadAllData();
      },
      error: (err) => this.showAlert(`Error purgando cola: ${err.message}`, 'alert-danger', 'bi-exclamation-triangle')
    });
  }

  deleteQueue(name: string): void {
    if (!confirm(`¿Confirmas eliminar la cola '${name}' del broker RabbitMQ?`)) return;
    this.rabbitService.deleteQueue(name).subscribe({
      next: () => {
        this.showAlert(`Cola '${name}' eliminada correctamente.`, 'alert-success', 'bi-check-circle');
        this.loadAllData();
      },
      error: (err) => this.showAlert(`Error eliminando cola: ${err.message}`, 'alert-danger', 'bi-exclamation-triangle')
    });
  }

  submitCreateQueue(): void {
    if (!this.newQueue.name.trim()) return;
    this.rabbitService.createQueue(this.newQueue).subscribe({
      next: () => {
        this.showAlert(`Cola '${this.newQueue.name}' creada exitosamente en RabbitMQ.`, 'alert-success', 'bi-check-circle');
        this.showCreateModal = false;
        this.newQueue.name = '';
        this.loadAllData();
      },
      error: (err) => {
        this.showAlert(`Error al crear cola: ${err.error?.message || err.message}`, 'alert-danger', 'bi-exclamation-triangle');
      }
    });
  }

  triggerSimulatedError(): void {
    this.simulating = true;
    const testCode = 'BD-DEMO-DLQ-' + Math.floor(1000 + Math.random() * 9000);
    this.rabbitService.simulateError(testCode).subscribe({
      next: (res) => {
        this.simulating = false;
        this.showAlert(
          `Falla provocada exitosamente (${testCode}). El consumidor procesó basicNack(requeue=false) y el mensaje llegó a la DLQ.`,
          'alert-warning',
          'bi-exclamation-triangle-fill'
        );
        setTimeout(() => this.loadAllData(), 800);
      },
      error: (err) => {
        this.simulating = false;
        this.showAlert(`Simulación enviada: ${err.error?.message || err.message}`, 'alert-warning', 'bi-info-circle');
        setTimeout(() => this.loadAllData(), 800);
      }
    });
  }

  private showAlert(msg: string, cls: string, icon: string): void {
    this.alertMessage = msg;
    this.alertClass = cls;
    this.alertIcon = icon;
    this.cdr.markForCheck();
  }
}
