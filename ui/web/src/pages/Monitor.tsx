import React, { useEffect, useState } from 'react';
import { Card, Table, Tag, Button, Space, Select, Row, Col, Statistic, Spin, Badge } from 'antd';
import { PlayCircleOutlined, ReloadOutlined, CheckCircleOutlined, CloseCircleOutlined, SyncOutlined } from '@ant-design/icons';
import { executionApi } from '../api/execution';
import type { Execution, ExecutionMetrics, ExecutionStatus } from '../types/execution';

const Monitor: React.FC = () => {
  const [executions, setExecutions] = useState<Execution[]>([]);
  const [metrics, setMetrics] = useState<ExecutionMetrics | null>(null);
  const [loading, setLoading] = useState(false);
  const [statusFilter, setStatusFilter] = useState<string>('all');

  useEffect(() => {
    loadData();
  }, [statusFilter]);

  const loadData = async () => {
    try {
      setLoading(true);
      const [executionsData, metricsData] = await Promise.all([
        executionApi.list(statusFilter !== 'all' ? { status: statusFilter } : undefined),
        executionApi.metrics(),
      ]);
      setExecutions(executionsData.executions);
      setMetrics(metricsData);
    } catch (error) {
      console.error('Failed to load data:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleCancel = async (id: string) => {
    try {
      await executionApi.cancel(id);
      loadData();
    } catch {
      console.error('Cancel failed');
    }
  };

  const handleRetry = async (id: string) => {
    try {
      await executionApi.retry(id);
      loadData();
    } catch {
      console.error('Retry failed');
    }
  };

  const getStatusColor = (status: ExecutionStatus) => {
    const colorMap: Record<ExecutionStatus, string> = {
      pending: 'orange',
      running: 'blue',
      success: 'green',
      failed: 'red',
      cancelled: 'gray',
    };
    return colorMap[status] || 'default';
  };

  const getStatusIcon = (status: ExecutionStatus) => {
    switch (status) {
      case 'running':
        return <SyncOutlined spin />;
      case 'success':
        return <CheckCircleOutlined />;
      case 'failed':
        return <CloseCircleOutlined />;
      default:
        return null;
    }
  };

  const columns = [
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      render: (status: ExecutionStatus) => (
        <Tag color={getStatusColor(status)} icon={getStatusIcon(status)}>
          {status}
        </Tag>
      ),
    },
    { title: '管道名称', dataIndex: 'pipelineName', key: 'pipelineName' },
    {
      title: '触发方式',
      dataIndex: 'triggerType',
      key: 'triggerType',
      render: (type: string) => {
        const typeMap: Record<string, string> = { manual: '手动', scheduled: '定时', api: 'API' };
        return typeMap[type] || type;
      },
    },
    { title: '开始时间', dataIndex: 'startTime', key: 'startTime' },
    {
      title: '耗时',
      dataIndex: 'duration',
      key: 'duration',
      render: (duration?: number) => duration ? `${duration}s` : '-',
    },
    {
      title: '进度',
      dataIndex: 'progress',
      key: 'progress',
      render: (progress?: number) => progress !== undefined ? `${progress}%` : '-',
    },
    {
      title: '操作',
      key: 'action',
      render: (_: unknown, record: Execution) => (
        <Space size="small">
          {record.status === 'running' && (
            <Button size="small" danger onClick={() => handleCancel(record.id)}>
              取消
            </Button>
          )}
          {record.status === 'failed' && (
            <Button size="small" onClick={() => handleRetry(record.id)}>
              重试
            </Button>
          )}
        </Space>
      ),
    },
  ];

  if (loading && executions.length === 0) {
    return (
      <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: 400 }}>
        <Spin size="large" />
      </div>
    );
  }

  return (
    <div>
      <Row gutter={16} style={{ marginBottom: 24 }}>
        <Col span={6}>
          <Card>
            <Statistic
              title="执行总数"
              value={metrics?.totalExecutions || 0}
              prefix={<PlayCircleOutlined />}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="今日执行"
              value={metrics?.executionsToday || 0}
              valueStyle={{ color: '#1890ff' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="成功率"
              value={metrics?.successRate || 0}
              suffix="%"
              valueStyle={{ color: '#3f8600' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="正在运行"
              value={metrics?.runningCount || 0}
              prefix={<Badge status="processing" />}
              valueStyle={{ color: '#1890ff' }}
            />
          </Card>
        </Col>
      </Row>

      <Card
        title="执行历史"
        extra={
          <Space>
            <Select
              value={statusFilter}
              onChange={setStatusFilter}
              style={{ width: 120 }}
            >
              <Select.Option value="all">全部</Select.Option>
              <Select.Option value="running">运行中</Select.Option>
              <Select.Option value="success">成功</Select.Option>
              <Select.Option value="failed">失败</Select.Option>
              <Select.Option value="pending">等待中</Select.Option>
              <Select.Option value="cancelled">已取消</Select.Option>
            </Select>
            <Button icon={<ReloadOutlined />} onClick={loadData}>
              刷新
            </Button>
          </Space>
        }
      >
        <Table
          columns={columns}
          dataSource={executions}
          rowKey="id"
          loading={loading}
          pagination={{ pageSize: 10 }}
        />
      </Card>
    </div>
  );
};

export default Monitor;
