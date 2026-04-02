import React, { useEffect, useState, useCallback } from 'react';
import { Card, Button, Space, message, Tabs, DragableNodeList, Empty } from 'antd';
import { SaveOutlined, CheckCircleOutlined, ArrowLeftOutlined } from '@ant-design/icons';
import { useParams, useNavigate } from 'react-router-dom';
import { pipelineApi } from '../api/pipeline';
import DAGCanvas from '../components/DAGCanvas';
import NodeConfig from '../components/NodeConfig';
import type { Pipeline, PipelineNode, PipelineEdge } from '../types/pipeline';
import type { NodeTemplate } from '../types/function';

const nodeTemplates: NodeTemplate[] = [
  { type: 'source', label: '数据源', icon: '📥', defaultConfig: {}, description: '从外部系统读取数据' },
  { type: 'transform', label: '转换', icon: '🔄', defaultConfig: {}, description: '数据格式转换' },
  { type: 'filter', label: '过滤', icon: '🔍', defaultConfig: {}, description: '根据条件过滤数据' },
  { type: 'aggregate', label: '聚合', icon: '📊', defaultConfig: {}, description: '数据聚合计算' },
  { type: 'join', label: '连接', icon: '🔗', defaultConfig: {}, description: '多数据源连接' },
  { type: 'sink', label: '数据目标', icon: '📤', defaultConfig: {}, description: '输出到外部系统' },
  { type: 'function', label: '函数', icon: '⚙️', defaultConfig: {}, description: '自定义函数处理' },
];

const PipelineEditor: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [pipeline, setPipeline] = useState<Pipeline | null>(null);
  const [nodes, setNodes] = useState<PipelineNode[]>([]);
  const [edges, setEdges] = useState<PipelineEdge[]>([]);
  const [selectedNode, setSelectedNode] = useState<PipelineNode | null>(null);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (id && id !== 'new') {
      loadPipeline();
    }
  }, [id]);

  const loadPipeline = async () => {
    if (!id || id === 'new') return;
    try {
      setLoading(true);
      const data = await pipelineApi.get(id);
      setPipeline(data);
      setNodes(data.nodes || []);
      setEdges(data.edges || []);
    } catch {
      message.error('加载管道失败');
    } finally {
      setLoading(false);
    }
  };

  const handleAddNode = (type: string) => {
    const template = nodeTemplates.find(t => t.type === type);
    if (!template) return;

    const newNode: PipelineNode = {
      id: `node_${Date.now()}`,
      type: template.type,
      name: `${template.label}_${nodes.length + 1}`,
      config: { ...template.defaultConfig },
      position: {
        x: 100 + (nodes.length % 5) * 150,
        y: 100 + Math.floor(nodes.length / 5) * 100,
      },
    };
    setNodes([...nodes, newNode]);
  };

  const handleNodeClick = (node: PipelineNode) => {
    setSelectedNode(node);
  };

  const handleNodeUpdate = (nodeId: string, config: Record<string, unknown>) => {
    setNodes(nodes.map(n => (n.id === nodeId ? { ...n, config } : n)));
    if (selectedNode?.id === nodeId) {
      setSelectedNode({ ...selectedNode, config });
    }
  };

  const handleSave = async () => {
    try {
      setSaving(true);
      if (pipeline) {
        await pipelineApi.update({ id: pipeline.id, nodes, edges, name: pipeline.name });
        message.success('保存成功');
      } else {
        await pipelineApi.create({ name: '新管道', nodes, edges });
        message.success('创建成功');
      }
    } catch {
      message.error('保存失败');
    } finally {
      setSaving(false);
    }
  };

  const handleValidate = async () => {
    if (!pipeline) {
      message.warning('请先保存管道');
      return;
    }
    try {
      const result = await pipelineApi.validate({ name: pipeline.name, nodes, edges });
      if (result.valid) {
        message.success('管道验证通过');
      } else {
        message.error(`验证失败: ${result.errors.join(', ')}`);
      }
    } catch {
      message.error('验证请求失败');
    }
  };

  const handleDeleteNode = (nodeId: string) => {
    setNodes(nodes.filter(n => n.id !== nodeId));
    setEdges(edges.filter(e => e.source !== nodeId && e.target !== nodeId));
    if (selectedNode?.id === nodeId) {
      setSelectedNode(null);
    }
  };

  return (
    <div style={{ display: 'flex', gap: 16 }}>
      <Card title="节点工具箱" style={{ width: 200 }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          {nodeTemplates.map(template => (
            <Button
              key={template.type}
              onClick={() => handleAddNode(template.type)}
              block
            >
              {template.icon} {template.label}
            </Button>
          ))}
        </div>
      </Card>

      <div style={{ flex: 1 }}>
        <Card
          title={pipeline?.name || '新管道'}
          extra={
            <Space>
              <Button icon={<ArrowLeftOutlined />} onClick={() => navigate('/pipeline')}>
                返回
              </Button>
              <Button icon={<CheckCircleOutlined />} onClick={handleValidate}>
                验证
              </Button>
              <Button type="primary" icon={<SaveOutlined />} onClick={handleSave} loading={saving}>
                保存
              </Button>
            </Space>
          }
          bodyStyle={{ padding: 0 }}
        >
          {nodes.length === 0 ? (
            <div style={{ padding: 40 }}>
              <Empty description="请点击左侧节点按钮添加节点到画布" />
            </div>
          ) : (
            <DAGCanvas
              nodes={nodes}
              edges={edges}
              onNodeClick={handleNodeClick}
              selectedNodeId={selectedNode?.id}
              editable
            />
          )}
        </Card>
      </div>

      <Card title="属性" style={{ width: 300 }}>
        {selectedNode ? (
          <div>
            <NodeConfig node={selectedNode} onUpdate={handleNodeUpdate} />
            <Button
              type="link"
              danger
              onClick={() => handleDeleteNode(selectedNode.id)}
              style={{ width: '100%', marginTop: 16 }}
            >
              删除节点
            </Button>
          </div>
        ) : (
          <div style={{ color: '#999', textAlign: 'center', padding: 40 }}>
            请选择一个节点
          </div>
        )}
      </Card>
    </div>
  );
};

export default PipelineEditor;
