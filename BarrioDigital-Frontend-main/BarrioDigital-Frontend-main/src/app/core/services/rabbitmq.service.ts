import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from '../../../environments/environment';

export interface QueueInfo {
  name: string;
  messageCount: number;
  consumerCount: number;
  durable: boolean;
  exclusive: boolean;
  autoDelete: boolean;
  arguments?: Record<string, any>;
  status?: string;
}

export interface DlqEvent {
  queue: string;
  reason: string;
  payload: string;
  timestamp: string;
}

export interface CreateQueueRequest {
  name: string;
  durable: boolean;
  exclusive: boolean;
  autoDelete: boolean;
  deadLetterExchange?: string;
  deadLetterRoutingKey?: string;
}

@Injectable({
  providedIn: 'root'
})
export class RabbitMqService {
  private readonly baseUrl = `${environment.apiUrl}/admin`;

  constructor(private http: HttpClient) {}

  getQueues(): Observable<QueueInfo[]> {
    return this.http.get<QueueInfo[]>(`${this.baseUrl}/queues`);
  }

  createQueue(dto: CreateQueueRequest): Observable<QueueInfo> {
    return this.http.post<QueueInfo>(`${this.baseUrl}/queues`, dto);
  }

  deleteQueue(queueName: string): Observable<any> {
    return this.http.delete(`${this.baseUrl}/queues/${encodeURIComponent(queueName)}`);
  }

  purgeQueue(queueName: string): Observable<any> {
    return this.http.post(`${this.baseUrl}/queues/${encodeURIComponent(queueName)}/purge`, {});
  }

  getDlqEvents(): Observable<DlqEvent[]> {
    return this.http.get<DlqEvent[]>(`${this.baseUrl}/dlq/events`);
  }

  simulateError(trackingNumber: string): Observable<any> {
    return this.http.post(`${this.baseUrl}/test/simulate-error?trackingNumber=${encodeURIComponent(trackingNumber)}`, {});
  }
}
