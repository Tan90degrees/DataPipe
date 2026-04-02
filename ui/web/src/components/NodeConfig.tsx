import React from 'react';
import { Form, Input, Select, InputNumber, Switch, Card, Divider } from 'antd';
import type { PipelineNode } from '../types/pipeline';

interface NodeConfigProps {
  node: PipelineNode | null;
  onUpdate?: (nodeId: string, config: Record<string, unknown>) => void;
  readOnly?: boolean;
}

const NodeConfig: React.FC<NodeConfigProps> = ({ node, onUpdate, readOnly = false }) => {
  if (!node) {
    return (
      <Card title="节点配置">
        <div style={{ color: '#999', textAlign: 'center', padding: 40 }}>
          请选择一个节点进行配置
        </div>
      </Card>
    );
  }

  const handleConfigChange = (key: string, value: unknown) => {
    if (onUpdate) {
      onUpdate(node.id, { ...node.config, [key]: value });
    }
  };

  return (
    <Card
      title={`节点配置: ${node.name}`}
      extra={<span style={{ color: '#999' }}>类型: {node.type}</span>}
    >
      <Form layout="vertical" disabled={readOnly}>
        <Form.Item label="节点名称">
          <Input
            value={node.name}
            onChange={(e) => handleConfigChange('name', e.target.value)}
          />
        </Form.Item>

        <Divider>配置参数</Divider>

        {node.type === 'source' && (
          <>
            <Form.Item label="数据源类型">
              <Select
                value={(node.config.sourceType as string) || 'database'}
                onChange={(value) => handleConfigChange('sourceType', value)}
              >
                <Select.Option value="database">数据库</Select.Option>
                <Select.Option value="file">文件</Select.Option>
                <Select.Option value="api">API</Select.Option>
                <Select.Option value="stream">流数据</Select.Option>
              </Select>
            </Form.Item>
            <Form.Item label="连接字符串">
              <Input
                value={node.config.connectionString as string}
                onChange={(e) => handleConfigChange('connectionString', e.target.value)}
                placeholder="请输入连接字符串"
              />
            </Form.Item>
            <Form.Item label="查询语句">
              <Input.TextArea
                value={node.config.query as string}
                onChange={(e) => handleConfigChange('query', e.target.value)}
                rows={3}
                placeholder="SELECT * FROM ..."
              />
            </Form.Item>
          </>
        )}

        {node.type === 'transform' && (
          <>
            <Form.Item label="转换类型">
              <Select
                value={(node.config.transformType as string) || 'map'}
                onChange={(value) => handleConfigChange('transformType', value)}
              >
                <Select.Option value="map">映射</Select.Option>
                <Select.Option value="flatten">扁平化</Select.Option>
                <Select.Option value="rename">重命名</Select.Option>
                <Select.Option value="convert">类型转换</Select.Option>
              </Select>
            </Form.Item>
            <Form.Item label="转换表达式">
              <Input.TextArea
                value={node.config.expression as string}
                onChange={(e) => handleConfigChange('expression', e.target.value)}
                rows={3}
                placeholder="field1 -> newField"
              />
            </Form.Item>
          </>
        )}

        {node.type === 'filter' && (
          <>
            <Form.Item label="过滤条件">
              <Input.TextArea
                value={node.config.condition as string}
                onChange={(e) => handleConfigChange('condition', e.target.value)}
                rows={3}
                placeholder="field > 100"
              />
            </Form.Item>
            <Form.Item label="保留符合条件的记录">
              <Switch
                checked={node.config.keepMatches as boolean}
                onChange={(checked) => handleConfigChange('keepMatches', checked)}
              />
            </Form.Item>
          </>
        )}

        {node.type === 'sink' && (
          <>
            <Form.Item label="目标类型">
              <Select
                value={(node.config.sinkType as string) || 'database'}
                onChange={(value) => handleConfigChange('sinkType', value)}
              >
                <Select.Option value="database">数据库</Select.Option>
                <Select.Option value="file">文件</Select.Option>
                <Select.Option value="api">API</Select.Option>
              </Select>
            </Form.Item>
            <Form.Item label="目标地址">
              <Input
                value={node.config.target as string}
                onChange={(e) => handleConfigChange('target', e.target.value)}
                placeholder="表名或文件路径"
              />
            </Form.Item>
            <Form.Item label="批处理大小">
              <InputNumber
                value={node.config.batchSize as number}
                onChange={(value) => handleConfigChange('batchSize', value)}
                min={1}
                max={10000}
              />
            </Form.Item>
          </>
        )}

        {(node.type === 'aggregate' || node.type === 'join' || node.type === 'function') && (
          <Form.Item label="配置">
            <Input.TextArea
              value={JSON.stringify(node.config, null, 2)}
              onChange={(e) => {
                try {
                  handleConfigChange('config', JSON.parse(e.target.value));
                } catch {
                  // ignore
                }
              }}
              rows={6}
            />
          </Form.Item>
        )}
      </Form>
    </Card>
  );
};

export default NodeConfig;
