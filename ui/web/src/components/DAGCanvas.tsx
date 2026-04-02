import React, { useCallback, useState } from 'react';
import { Card, Empty, Spin } from 'antd';
import type { PipelineNode, PipelineEdge } from '../types/pipeline';

interface DAGCanvasProps {
  nodes: PipelineNode[];
  edges: PipelineEdge[];
  onNodeClick?: (node: PipelineNode) => void;
  onEdgeClick?: (edge: PipelineEdge) => void;
  selectedNodeId?: string;
  selectedEdgeId?: string;
  editable?: boolean;
}

const DAGCanvas: React.FC<DAGCanvasProps> = ({
  nodes,
  edges,
  onNodeClick,
  onEdgeClick,
  selectedNodeId,
  selectedEdgeId,
  editable = false,
}) => {
  const [loading] = useState(false);

  const getNodeIcon = (type: string) => {
    const iconMap: Record<string, string> = {
      source: '📥',
      transform: '🔄',
      filter: '🔍',
      aggregate: '📊',
      join: '🔗',
      sink: '📤',
      function: '⚙️',
    };
    return iconMap[type] || '📦';
  };

  const getNodePosition = (nodeId: string) => {
    const node = nodes.find((n) => n.id === nodeId);
    return node?.position || { x: 0, y: 0 };
  };

  const getEdgePath = (source: string, target: string) => {
    const sourcePos = getNodePosition(source);
    const targetPos = getNodePosition(target);
    const midX = (sourcePos.x + targetPos.x) / 2;
    const midY = (sourcePos.y + targetPos.y) / 2;
    return `M ${sourcePos.x + 60} ${sourcePos.y + 20} Q ${midX} ${midY} ${targetPos.x} ${targetPos.y + 20}`;
  };

  if (loading) {
    return (
      <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: 400 }}>
        <Spin size="large" />
      </div>
    );
  }

  if (nodes.length === 0) {
    return (
      <Card>
        <Empty description="暂无节点，请从左侧拖拽添加节点" />
      </Card>
    );
  }

  return (
    <Card bodyStyle={{ padding: 0, position: 'relative', minHeight: 400 }}>
      <svg width="100%" height="400" style={{ background: '#fafafa' }}>
        <defs>
          <marker
            id="arrowhead"
            markerWidth="10"
            markerHeight="7"
            refX="9"
            refY="3.5"
            orient="auto"
          >
            <polygon points="0 0, 10 3.5, 0 7" fill="#1890ff" />
          </marker>
        </defs>

        {edges.map((edge) => (
          <g key={edge.id}>
            <path
              d={getEdgePath(edge.source, edge.target)}
              stroke={selectedEdgeId === edge.id ? '#1890ff' : '#d9d9d9'}
              strokeWidth={selectedEdgeId === edge.id ? 2 : 1}
              fill="none"
              markerEnd="url(#arrowhead)"
              onClick={() => onEdgeClick?.(edge)}
              style={{ cursor: 'pointer' }}
            />
            {edge.label && (
              <text
                x={(getNodePosition(edge.source).x + getNodePosition(edge.target).x) / 2 + 30}
                y={(getNodePosition(edge.source).y + getNodePosition(edge.target).y) / 2 + 20}
                fontSize={12}
                fill="#666"
              >
                {edge.label}
              </text>
            )}
          </g>
        ))}

        {nodes.map((node) => (
          <g
            key={node.id}
            transform={`translate(${node.position.x}, ${node.position.y})`}
            onClick={() => onNodeClick?.(node)}
            style={{ cursor: 'pointer' }}
          >
            <rect
              width={120}
              height={40}
              rx={4}
              fill={selectedNodeId === node.id ? '#e6f7ff' : 'white'}
              stroke={selectedNodeId === node.id ? '#1890ff' : '#d9d9d9'}
              strokeWidth={selectedNodeId === node.id ? 2 : 1}
            />
            <text x={60} y={14} textAnchor="middle" fontSize={12}>
              {getNodeIcon(node.type)}
            </text>
            <text x={60} y={30} textAnchor="middle" fontSize={12} fill="#333">
              {node.name}
            </text>
          </g>
        ))}
      </svg>

      {editable && (
        <div style={{ position: 'absolute', bottom: 16, right: 16, color: '#999', fontSize: 12 }}>
          提示：拖拽节点到画布中，连接节点创建管道
        </div>
      )}
    </Card>
  );
};

export default DAGCanvas;
