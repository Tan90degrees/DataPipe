export interface PipelineNode {
  id: string;
  type: string;
  name: string;
  config: Record<string, unknown>;
  position: { x: number; y: number };
}

export interface PipelineEdge {
  id: string;
  source: string;
  target: string;
  label?: string;
}

export interface Pipeline {
  id: string;
  name: string;
  description?: string;
  nodes: PipelineNode[];
  edges: PipelineEdge[];
  status: 'draft' | 'active' | 'paused' | 'error';
  createdAt: string;
  updatedAt: string;
}

export interface CreatePipelineRequest {
  name: string;
  description?: string;
  nodes: PipelineNode[];
  edges: PipelineEdge[];
}

export interface UpdatePipelineRequest extends Partial<CreatePipelineRequest> {
  id: string;
}

export interface PipelineListResponse {
  pipelines: Pipeline[];
  total: number;
  page: number;
  pageSize: number;
}
