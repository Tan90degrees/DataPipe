import React, { useEffect, useState } from 'react';
import { Card, Row, Col, Statistic, Table, Tag, Spin } from 'antd';
import { ArrowUpOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import { systemApi } from '../api/system';

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

const Dashboard: React.FC = () => {
  const navigate = useNavigate();
  const [loading, setLoading] = useState(true);
  const [stats, setStats] = useState<DashboardStats>({
    totalPipelines: 0,
    activePipelines: 0,
    totalExecutions: 0,
    successRate: 0,
    runningPipelines: 0,
    recentExecutions: [],
  });

  useEffect(() => {
    loadDashboardData();
  }, []);

  const loadDashboardData = async () => {
    try {
      setLoading(true);
      const data = await systemApi.getDashboardStats();
      setStats(data);
    } catch (error) {
      console.error('Failed to load dashboard data:', error);
    } finally {
      setLoading(false);
    }
  };

  const getStatusColor = (status: string) => {
    const colorMap: Record<string, string> = {
      success: 'green',
      failed: 'red',
      running: 'blue',
      pending: 'orange',
      cancelled: 'gray',
    };
    return colorMap[status] || 'default';
  };

  const columns = [
    { title: '管道名称', dataIndex: 'pipelineName', key: 'pipelineName' },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => <Tag color={getStatusColor(status)}>{status}</Tag>,
    },
    { title: '开始时间', dataIndex: 'startTime', key: 'startTime' },
    {
      title: '耗时',
      dataIndex: 'duration',
      key: 'duration',
      render: (duration?: number) => duration ? `${duration}s` : '-',
    },
  ];

  if (loading) {
    return (
      <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: 400 }}>
        <Spin size="large" />
      </div>
    );
  }

  return (
    <div>
      <Row gutter={16}>
        <Col span={6}>
          <Card>
            <Statistic
              title="总管道数"
              value={stats.totalPipelines}
              valueStyle={{ color: '#3f8600' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="活跃管道"
              value={stats.activePipelines}
              valueStyle={{ color: '#1890ff' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="执行总数"
              value={stats.totalExecutions}
              valueStyle={{ color: '#cf1322' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="成功率"
              value={stats.successRate}
              suffix="%"
              valueStyle={{ color: stats.successRate >= 90 ? '#3f8600' : '#cf1322' }}
            />
          </Card>
        </Col>
      </Row>

      <Row gutter={16} style={{ marginTop: 24 }}>
        <Col span={12}>
          <Card
            title="运行中的管道"
            extra={<a onClick={() => navigate('/monitor')}>查看更多</a>}
          >
            <Statistic
              value={stats.runningPipelines}
              prefix={<ArrowUpOutlined />}
              valueStyle={{ color: '#1890ff' }}
            />
          </Card>
        </Col>
        <Col span={12}>
          <Card title="管道状态分布">
            <Row gutter={16}>
              <Col span={8}>
                <Statistic title="草稿" value={stats.totalPipelines - stats.activePipelines} />
              </Col>
              <Col span={8}>
                <Statistic title="活跃" value={stats.activePipelines} />
              </Col>
              <Col span={8}>
                <Statistic title="暂停" value={0} />
              </Col>
            </Row>
          </Card>
        </Col>
      </Row>

      <Row gutter={16} style={{ marginTop: 24 }}>
        <Col span={24}>
          <Card
            title="最近执行"
            extra={<a onClick={() => navigate('/monitor')}>查看更多</a>}
          >
            <Table
              columns={columns}
              dataSource={stats.recentExecutions}
              rowKey="id"
              pagination={false}
              size="small"
            />
          </Card>
        </Col>
      </Row>
    </div>
  );
};

export default Dashboard;
