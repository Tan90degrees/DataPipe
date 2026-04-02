export type ExecutionStatus = 'pending' | 'running' | 'success' | 'failed' | 'cancelled';

export interface Execution {
  id: string;
  pipelineId: string;
  pipelineName: string;
  status: ExecutionStatus;
  startTime: string;
  endTime?: string;
  duration?: number;
  triggerType: 'manual' | 'scheduled' | 'api';
  error?: string;
  progress?: number;
}

export interface ExecutionLog {
  id: string;
  executionId: string;
  nodeId: string;
  nodeName: string;
  level: 'info' | 'warning' | 'error';
  message: string;
  timestamp: string;
}

export interface ExecutionMetrics {
  totalExecutions: number;
  successRate: number;
  averageDuration: number;
  executionsToday: number;
  runningCount: number;
}

export interface ExecutionListResponse {
  executions: Execution[];
  total: number;
  page: number;
  pageSize: number;
}
