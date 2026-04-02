import apiClient from './client';
import type { Execution, ExecutionLog, ExecutionMetrics, ExecutionListResponse } from '../types/execution';

export const executionApi = {
  list: (params?: { page?: number; pageSize?: number; pipelineId?: string; status?: string }) => {
    return apiClient.get<ExecutionListResponse>('/executions', { params });
  },

  get: (id: string) => {
    return apiClient.get<Execution>(`/executions/${id}`);
  },

  logs: (id: string, params?: { nodeId?: string; level?: string }) => {
    return apiClient.get<ExecutionLog[]>(`/executions/${id}/logs`, { params });
  },

  cancel: (id: string) => {
    return apiClient.post(`/executions/${id}/cancel`);
  },

  retry: (id: string) => {
    return apiClient.post<Execution>(`/executions/${id}/retry`);
  },

  metrics: () => {
    return apiClient.get<ExecutionMetrics>('/executions/metrics');
  },

  getProgress: (id: string) => {
    return apiClient.get<{ progress: number; currentNode?: string }>(`/executions/${id}/progress`);
  },

  searchLogs: (params: { executionId?: string; nodeId?: string; level?: string; keyword?: string; startTime?: string; endTime?: string; page?: number; pageSize?: number }) => {
    return apiClient.get<{ logs: ExecutionLog[]; total: number }>('/executions/logs/search', { params });
  },

  exportLogs: (params: { executionId?: string; startTime?: string; endTime?: string; format?: 'json' | 'csv' }) => {
    return apiClient.get('/executions/logs/export', { params, responseType: 'blob' });
  },
};
