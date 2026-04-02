import apiClient from './client';
import type { FunctionCategory, FunctionDefinition } from '../types/function';

interface SystemInfo {
  version: string;
  name: string;
  description: string;
}

interface DashboardStats {
  totalPipelines: number;
  activePipelines: number;
  totalExecutions: number;
  successRate: number;
  runningPipelines: number;
  recentExecutions: Array<{
    id: string;
    pipelineId: string;
    pipelineName: string;
    status: string;
    startTime: string;
    duration?: number;
  }>;
}

interface SystemSettings {
  theme: 'light' | 'dark';
  language: string;
  timezone: string;
  notificationEnabled: boolean;
  autoRefresh: boolean;
  refreshInterval: number;
}

export const systemApi = {
  getInfo: () => {
    return apiClient.get<SystemInfo>('/system/info');
  },

  getDashboardStats: () => {
    return apiClient.get<DashboardStats>('/system/dashboard');
  },

  getFunctions: () => {
    return apiClient.get<FunctionCategory[]>('/system/functions');
  },

  getFunctionDetails: (id: string) => {
    return apiClient.get<FunctionDefinition>(`/system/functions/${id}`);
  },

  getSettings: () => {
    return apiClient.get<SystemSettings>('/system/settings');
  },

  updateSettings: (data: Partial<SystemSettings>) => {
    return apiClient.put<SystemSettings>('/system/settings', data);
  },

  healthCheck: () => {
    return apiClient.get<{ status: string }>('/system/health');
  },
};
