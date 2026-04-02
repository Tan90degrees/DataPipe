import apiClient from './client';
import type { Pipeline, CreatePipelineRequest, UpdatePipelineRequest, PipelineListResponse } from '../types/pipeline';

export const pipelineApi = {
  list: (params?: { page?: number; pageSize?: number; status?: string }) => {
    return apiClient.get<PipelineListResponse>('/pipelines', { params });
  },

  get: (id: string) => {
    return apiClient.get<Pipeline>(`/pipelines/${id}`);
  },

  create: (data: CreatePipelineRequest) => {
    return apiClient.post<Pipeline>('/pipelines', data);
  },

  update: (data: UpdatePipelineRequest) => {
    return apiClient.put<Pipeline>(`/pipelines/${data.id}`, data);
  },

  delete: (id: string) => {
    return apiClient.delete(`/pipelines/${id}`);
  },

  validate: (data: CreatePipelineRequest) => {
    return apiClient.post<{ valid: boolean; errors: string[] }>('/pipelines/validate', data);
  },

  execute: (id: string, params?: { inputs?: Record<string, unknown> }) => {
    return apiClient.post<{ executionId: string }>(`/pipelines/${id}/execute`, params);
  },

  duplicate: (id: string) => {
    return apiClient.post<Pipeline>(`/pipelines/${id}/duplicate`);
  },

  export: (id: string) => {
    return apiClient.get(`/pipelines/${id}/export`, { responseType: 'blob' });
  },

  import: (file: File) => {
    const formData = new FormData();
    formData.append('file', file);
    return apiClient.post<Pipeline>('/pipelines/import', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
  },
};