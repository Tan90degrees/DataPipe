export interface FunctionDefinition {
  id: string;
  name: string;
  type: string;
  description: string;
  category: string;
  inputSchema?: Record<string, unknown>;
  outputSchema?: Record<string, unknown>;
  configSchema?: Record<string, unknown>;
}

export interface FunctionCategory {
  id: string;
  name: string;
  icon: string;
  functions: FunctionDefinition[];
}

export type NodeType = 
  | 'source'
  | 'transform'
  | 'filter'
  | 'aggregate'
  | 'join'
  | 'sink'
  | 'function';

export interface NodeTemplate {
  type: NodeType;
  label: string;
  icon: string;
  defaultConfig: Record<string, unknown>;
  description: string;
}
